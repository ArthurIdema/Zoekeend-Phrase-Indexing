import duckdb
from collections import Counter

def create_tokenizer_duckdb(con):
    con.sql("""
        CREATE TEMPORARY MACRO tokenize(s) AS (
            string_split_regex(regexp_replace(lower(strip_accents(CAST(s AS VARCHAR))), '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|''''"`-]+', ' ', 'g'), '\\s+')
        );
    """)

def extract_phrases(documents, n=2, min_freq=2, db_path='phrases.db'):
    con = duckdb.connect(database=db_path)
    create_tokenizer_duckdb(con)

    # Load documents into DuckDB table
    con.execute("CREATE TEMP TABLE docs AS SELECT * FROM (VALUES " +
                ",".join(["(?, ?)"] * len(documents)) +
                ") AS t(doc_id, text)", [item for pair in documents for item in pair])

    # Tokenize and flatten tokens in DuckDB
    tokens_df = con.sql("""
        SELECT doc_id, unnest(tokenize(text)) AS token
        FROM docs
    """).df()

    # Generate n-grams in Python
    token_counter = Counter()
    ngram_counter = Counter()

    grouped = tokens_df.groupby('doc_id')['token'].apply(list)

    total_tokens = 0
    for token_list in grouped:
        total_tokens += len(token_list)
        token_counter.update(token_list)
        ngrams = zip(*[token_list[i:] for i in range(n)])
        ngram_counter.update(ngrams)

    # Extract frequent phrases
    phrases = [" ".join(ngram) for ngram, freq in ngram_counter.items() if freq >= min_freq]
    return phrases

def extract_phrases_pmi_duckdb(con, fts_schema, n=2, min_freq=2, min_pmi=3.0):
    # 1. Create a tokenized table
    con.execute(f"""CREATE OR REPLACE TABLE {fts_schema}.tokens AS
        SELECT
            did AS doc_id,
            unnest({fts_schema}.tokenize(content)) AS token
        FROM
            documents;

    """)

    print("Tokenized documents:\n", con.execute(f"SELECT * FROM {fts_schema}.tokens LIMIT 10").fetchall())

    # 2. Add position index for each token in its document
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.tokens_pos AS
        SELECT doc_id, token,
               ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY rowid) AS pos
        FROM {fts_schema}.tokens
    """)

    # 3. Compute total token count
    total_tokens = con.execute(f"SELECT COUNT(*)::DOUBLE FROM {fts_schema}.tokens_pos").fetchone()[0]

    # 4. Compute token frequencies
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.token_freq AS
        SELECT token,
               COUNT(*) AS freq,
               COUNT(DISTINCT doc_id) AS doc_freq
        FROM {fts_schema}.tokens_pos
        GROUP BY token
    """)
    print("Token frequency:\n", con.execute(f"SELECT * FROM {fts_schema}.token_freq LIMIT 10").fetchall())

    # 5. Compute bigrams (or n-grams)
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.ngrams AS
        SELECT t1.token AS w1, t2.token AS w2,
               t1.doc_id AS doc_id
        FROM {fts_schema}.tokens_pos t1
        JOIN {fts_schema}.tokens_pos t2
        ON t1.doc_id = t2.doc_id AND t2.pos = t1.pos + 1
    """)

    # 6. Compute n-gram frequencies
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.ngram_freq AS
        SELECT w1, w2, COUNT(*) AS freq,
               COUNT(DISTINCT doc_id) AS doc_freq
        FROM {fts_schema}.ngrams
        GROUP BY w1, w2
        HAVING COUNT(*) >= {min_freq}
    """)
    
    print("N-gram frequency:\n", con.execute(f"SELECT * FROM {fts_schema}.ngram_freq LIMIT 10").fetchall())
    print(f"Number of n-grams: {con.execute(f'SELECT COUNT(*) FROM {fts_schema}.ngram_freq').fetchone()[0]}")
    # 7. Compute PMI for bigrams
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.phrases AS
        SELECT w1 || ' ' || w2 AS phrase,
            LOG(n.freq * {total_tokens} / (f1.freq * f2.freq)) / LOG(2) AS pmi,
            n.doc_freq AS df
        FROM {fts_schema}.ngram_freq n
        JOIN {fts_schema}.token_freq f1 ON n.w1 = f1.token
        JOIN {fts_schema}.token_freq f2 ON n.w2 = f2.token
        WHERE LOG(n.freq * {total_tokens} / (f1.freq * f2.freq)) / LOG(2) >= {min_pmi}
        ORDER BY pmi DESC
    """)

    print("Extracted phrases:\n", con.execute(f"SELECT phrase, pmi, df FROM {fts_schema}.phrases LIMIT 10").fetchall())
    print("Extracted tokens:\n", con.execute(f"SELECT token FROM {fts_schema}.token_freq LIMIT 10").fetchall())
    # 8. Combine phrases and words
    con.execute(f"""
        CREATE OR REPLACE TABLE {fts_schema}.dict AS
        SELECT ROW_NUMBER() OVER () AS termid, phrase as term, df
        FROM {fts_schema}.phrases
        WHERE NOT EXISTS (
            SELECT 1 FROM UNNEST(string_split(phrase, ' ')) AS word
            WHERE word.unnest IN (SELECT sw FROM {fts_schema}.stopwords)
        )
        UNION ALL
        SELECT ROW_NUMBER() OVER () + (SELECT COUNT(*) FROM {fts_schema}.phrases) AS termid, token AS term, doc_freq AS df
        FROM {fts_schema}.token_freq
        WHERE token NOT IN (SELECT sw FROM {fts_schema}.stopwords)
          AND freq >= {min_freq}
    """)
    
    print("Phrases:\n", con.execute(f"SELECT term, df FROM {fts_schema}.dict LIMIT 10").fetchall())

    con.execute(f"DROP TABLE IF EXISTS {fts_schema}.tokens_pos")
    con.execute(f"DROP TABLE IF EXISTS {fts_schema}.token_freq")
    con.execute(f"DROP TABLE IF EXISTS {fts_schema}.ngrams")
    con.execute(f"DROP TABLE IF EXISTS {fts_schema}.ngram_freq")
