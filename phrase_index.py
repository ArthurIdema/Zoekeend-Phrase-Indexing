import pathlib
import sys
import duckdb
import ir_datasets


from phrases_extractor import extract_phrases_pmi_duckdb
from ze_index import normalize

def insert_dataset(con, ir_dataset, logging=True):
    """
    Insert documents from an ir_dataset. Works with several datasets.
    Add document attributes if needed.
    """
    con.sql('CREATE TABLE documents (did TEXT, content TEXT)')
    insert = 'INSERT INTO documents(did, content) VALUES '
    sql = insert
    part = 0
    total = 0
    count = ir_dataset.docs_count()
    if logging:
        print(f"Inserting {count} docs...", file=sys.stderr)
    for doc in ir_dataset.docs_iter():
        doc_text = ""
        if hasattr(doc, 'title'):
            doc_text = doc.title
        if hasattr(doc, 'body'):
            doc_text += " " + doc.body
        if hasattr(doc, 'text'):
            doc_text += " " + doc.text
        sql += "('" + doc.doc_id + "','" + normalize(doc_text) + "'),"
        part += 1
        if part > 9999:
            total += part
            if logging:
                print(str(total) + " docs", file=sys.stderr)
            con.sql(sql)
            part = 0
            sql = insert
    con.sql(sql)

def create_lm(con, stemmer):
    con.sql(f"""
        CREATE OR REPLACE MACRO fts_main_documents.match_lm(query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS TABLE (
        WITH tokens AS (
            SELECT DISTINCT stem(unnest(fts_main_documents.tokenize(query_string)), '{stemmer}') AS t
        ),
        fieldids AS (
            SELECT fieldid
            FROM fts_main_documents.fields
            WHERE CASE WHEN ((fields IS NULL)) THEN (1) ELSE (field = ANY(SELECT * FROM (SELECT unnest(string_split(fields, ','))) AS fsq)) END
        ),
        qtermids AS (
            SELECT termid, df
            FROM fts_main_documents.dict AS dict, tokens
            WHERE (dict.term = tokens.t)
        ),
        qterms AS (
            SELECT termid, docid
            FROM fts_main_documents.terms AS terms
            WHERE (CASE WHEN ((fields IS NULL)) THEN (1) ELSE (fieldid = ANY(SELECT * FROM fieldids)) END
            AND (termid = ANY(SELECT qtermids.termid FROM qtermids)))
        ),
        term_tf AS (
            SELECT termid, docid, count_star() AS tf
            FROM qterms
            GROUP BY docid, termid
        ),
        cdocs AS (
            SELECT docid
            FROM qterms
            GROUP BY docid
            HAVING CASE WHEN (conjunctive) THEN ((count(DISTINCT termid) = (SELECT count_star() FROM tokens))) ELSE 1 END
        ),
        subscores AS (
           SELECT docs.docid, docs.len AS doc_len, term_tf.termid, term_tf.tf, qtermids.df, LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * docs.len)) AS subscore
           FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
           WHERE ((term_tf.docid = cdocs.docid)
           AND (term_tf.docid = docs.docid)
           AND (term_tf.termid = qtermids.termid))
        ),
        scores AS (
           SELECT docs.name AS docname, LN(MAX(doc_len)) + sum(subscore) AS score FROM subscores, fts_main_documents.docs AS docs WHERE subscores.docid = docs.docid GROUP BY docs.name
        ),
        postings_cost AS (
           SELECT COUNT(*) AS cost FROM term_tf
        )
        SELECT docname, score, (SELECT cost FROM postings_cost) AS postings_cost FROM scores
        );
    """)

def create_bm25(con, stemmer):
    con.sql(f"""
        CREATE MACRO fts_main_documents.match_bm25(docname, query_string, b := 0.75, conjunctive := 0, k := 1.2, fields := NULL) AS (
        WITH tokens AS (
            SELECT DISTINCT stem(unnest(fts_main_documents.tokenize(query_string)), '{stemmer}') AS t
        ),
        fieldids AS (
            SELECT fieldid
            FROM fts_main_documents.fields
            WHERE CASE WHEN ((fields IS NULL)) THEN (1) ELSE (field = ANY(SELECT * FROM (SELECT unnest(string_split(fields, ','))) AS fsq)) END
        ),
        qtermids AS (
            SELECT termid, df
            FROM fts_main_documents.dict AS dict, tokens
            WHERE (dict.term = tokens.t)
        ),
        qterms AS (
            SELECT termid, docid
            FROM fts_main_documents.terms AS terms
            WHERE (CASE WHEN ((fields IS NULL)) THEN (1) ELSE (fieldid = ANY(SELECT * FROM fieldids)) END
            AND (termid = ANY(SELECT qtermids.termid FROM qtermids)))
        ),
        term_tf AS (
            SELECT termid, docid, count_star() AS tf
            FROM qterms
            GROUP BY docid, termid
        ),
        cdocs AS (
            SELECT docid
            FROM qterms
            GROUP BY docid
            HAVING CASE WHEN (conjunctive) THEN ((count(DISTINCT termid) = (SELECT count_star() FROM tokens))) ELSE 1 END
        ),
        subscores AS (
           SELECT docs.docid, docs.len, term_tf.termid, term_tf.tf, qtermids.df, (log((((((SELECT num_docs FROM fts_main_documents.stats) - df) + 0.5) / (df + 0.5)) + 1)) * ((tf * (k + 1)) / (tf + (k * ((1 - b) + (b * (len / (SELECT avgdl FROM fts_main_documents.stats)))))))) AS subscore
           FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
           WHERE ((term_tf.docid = cdocs.docid)
           AND (term_tf.docid = docs.docid)
           AND (term_tf.termid = qtermids.termid))
        ),
        scores AS (
           SELECT docid, sum(subscore) AS score FROM subscores GROUP BY docid
        ),
        SELECT score FROM scores, fts_main_documents.docs AS docs
        WHERE ((scores.docid = docs.docid) AND (docs."name" = docname)))
    """)

def create_docs_table(con, fts_schema="fts_main_documents", input_schema="main", input_table="documents", input_id="did"):
    """
    Create the documents table.
    input_id should be the column name in input_table that uniquely identifies each document (e.g., 'did').
    """
    con.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {fts_schema};
        CREATE TABLE {fts_schema}.docs AS (
            SELECT
                row_number() OVER () AS docid,
                {input_id} AS name
            FROM
                {input_schema}.{input_table}
        );
    """)

def create_tokenizer_duckdb(con):
    con.sql("""
        CREATE MACRO fts_main_documents.tokenize(s) AS (
            string_split_regex(regexp_replace(lower(strip_accents(CAST(s AS VARCHAR))), '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|''''"`-]+', ' ', 'g'), '\\s+')
        );
    """)

def create_tokenizer_ciff(con, fts_schema="fts_main_documents"):
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {fts_schema}.dict (termid BIGINT, term TEXT, df BIGINT);
        CREATE OR REPLACE MACRO {fts_schema}.tokenize(query_string) AS (
          WITH RECURSIVE sequence AS (
            SELECT range AS nr 
            FROM RANGE((SELECT MAX(LEN(term)) + 1 FROM {fts_schema}.dict))
          ),
          simpledict AS (
            SELECT '' AS term
            UNION
            SELECT term FROM {fts_schema}.dict
          ),
          subterms(term, subquery) AS (
            SELECT '', lower(strip_accents(CAST(query_string AS VARCHAR)))
            UNION
            SELECT MAX(dict.term), SUBSTRING(subquery,
              CASE WHEN MAX(nr) < 1 THEN 2 ELSE MAX(nr) + 1 END,
              LEN(subquery)) AS subquery
            FROM subterms, sequence, simpledict as dict
            WHERE SUBSTRING(subquery, 1, nr) = dict.term
            GROUP BY subquery
         )
         SELECT LIST(term)  FROM subterms WHERE NOT term = ''
       )
    """)

def create_stopwords_table(con, fts_schema="fts_main_documents", stopwords='none'):
    """
    Create the stopwords table.
    If stopwords is 'english', it will create a table with English stopwords. 
    If stopwords is 'none', it will create an empty table.
    """
    con.sql(f"DROP TABLE IF EXISTS {fts_schema}.stopwords;")
    if stopwords == 'english':
        con.sql(f"""
                CREATE TABLE {fts_schema}.stopwords (sw VARCHAR);
                INSERT INTO {fts_schema}.stopwords VALUES ('a'), ('a''s'), ('able'), ('about'), ('above'), ('according'), ('accordingly'), ('across'), ('actually'), ('after'), ('afterwards'), ('again'), ('against'), ('ain''t'), ('all'), ('allow'), ('allows'), ('almost'), ('alone'), ('along'), ('already'), ('also'), ('although'), ('always'), ('am'), ('among'), ('amongst'), ('an'), ('and'), ('another'), ('any'), ('anybody'), ('anyhow'), ('anyone'), ('anything'), ('anyway'), ('anyways'), ('anywhere'), ('apart'), ('appear'), ('appreciate'), ('appropriate'), ('are'), ('aren''t'), ('around'), ('as'), ('aside'), ('ask'), ('asking'), ('associated'), ('at'), ('available'), ('away'), ('awfully'), ('b'), ('be'), ('became'), ('because'), ('become'), ('becomes'), ('becoming'), ('been'), ('before'), ('beforehand'), ('behind'), ('being'), ('believe'), ('below'), ('beside'), ('besides'), ('best'), ('better'), ('between'), ('beyond'), ('both'), ('brief'), ('but'), ('by'), ('c'), ('c''mon'), ('c''s'), ('came'), ('can'), ('can''t'), ('cannot'), ('cant'), ('cause'), ('causes'), ('certain'), ('certainly'), ('changes'), ('clearly'), ('co'), ('com'), ('come'), ('comes'), ('concerning'), ('consequently'), ('consider'), ('considering'), ('contain'), ('containing'), ('contains'), ('corresponding'), ('could'), ('couldn''t'), ('course'), ('currently'), ('d'), ('definitely'), ('described'), ('despite'), ('did'), ('didn''t'), ('different'), ('do'), ('does'), ('doesn''t'), ('doing'), ('don''t'), ('done'), ('down'), ('downwards'), ('during'), ('e'), ('each'), ('edu'), ('eg'), ('eight'), ('either'), ('else'), ('elsewhere'), ('enough'), ('entirely'), ('especially'), ('et'), ('etc'), ('even'), ('ever'), ('every'), ('everybody'), ('everyone'), ('everything'), ('everywhere'), ('ex'), ('exactly'), ('example'), ('except'), ('f'), ('far'), ('few'), ('fifth'), ('first'), ('five'), ('followed'), ('following'), ('follows'), ('for'), ('former'), ('formerly'), ('forth'), ('four'), ('from'), ('further'), ('furthermore'), ('g'), ('get'), ('gets'), ('getting'), ('given'), ('gives'), ('go'), ('goes'), ('going'), ('gone'), ('got'), ('gotten'), ('greetings'), ('h'), ('had'), ('hadn''t'), ('happens'), ('hardly'), ('has'), ('hasn''t'), ('have'), ('haven''t'), ('having'), ('he'), ('he''s'), ('hello'), ('help'), ('hence'), ('her'), ('here'), ('here''s'), ('hereafter'), ('hereby'), ('herein'), ('hereupon'), ('hers'), ('herself'), ('hi'), ('him'), ('himself'), ('his'), ('hither'), ('hopefully'), ('how'), ('howbeit'), ('however'), ('i'), ('i''d'), ('i''ll'), ('i''m'), ('i''ve'), ('ie'), ('if'), ('ignored'), ('immediate'), ('in'), ('inasmuch'), ('inc'), ('indeed'), ('indicate'), ('indicated'), ('indicates'), ('inner'), ('insofar'), ('instead'), ('into'), ('inward'), ('is'), ('isn''t'), ('it'), ('it''d'), ('it''ll'), ('it''s'), ('its'), ('itself'), ('j'), ('just'), ('k'), ('keep'), ('keeps'), ('kept'), ('know'), ('knows'), ('known'), ('l'), ('last'), ('lately'), ('later'), ('latter'), ('latterly'), ('least'), ('less'), ('lest'), ('let'), ('let''s'), ('like'), ('liked'), ('likely'), ('little'), ('look'), ('looking'), ('looks'), ('ltd'), ('m'), ('mainly'), ('many'), ('may'), ('maybe'), ('me'), ('mean'), ('meanwhile'), ('merely'), ('might'), ('more'), ('moreover'), ('most'), ('mostly'), ('much'), ('must'), ('my'), ('myself'), ('n'), ('name'), ('namely'), ('nd'), ('near'), ('nearly'), ('necessary'), ('need'), ('needs'), ('neither'), ('never'), ('nevertheless'), ('new'), ('next'), ('nine'), ('no'), ('nobody'), ('non'), ('none'), ('noone'), ('nor'), ('normally'), ('not'), ('nothing'), ('novel'), ('now'), ('nowhere'), ('o'), ('obviously'), ('of'), ('off'), ('often'), ('oh'), ('ok'), ('okay'), ('old'), ('on'), ('once'), ('one'), ('ones'), ('only'), ('onto'), ('or'), ('other'), ('others'), ('otherwise'), ('ought'), ('our'), ('ours'), ('ourselves'), ('out'), ('outside'), ('over'), ('overall'), ('own');
                INSERT INTO {fts_schema}.stopwords VALUES ('p'), ('particular'), ('particularly'), ('per'), ('perhaps'), ('placed'), ('please'), ('plus'), ('possible'), ('presumably'), ('probably'), ('provides'), ('q'), ('que'), ('quite'), ('qv'), ('r'), ('rather'), ('rd'), ('re'), ('really'), ('reasonably'), ('regarding'), ('regardless'), ('regards'), ('relatively'), ('respectively'), ('right'), ('s'), ('said'), ('same'), ('saw'), ('say'), ('saying'), ('says'), ('second'), ('secondly'), ('see'), ('seeing'), ('seem'), ('seemed'), ('seeming'), ('seems'), ('seen'), ('self'), ('selves'), ('sensible'), ('sent'), ('serious'), ('seriously'), ('seven'), ('several'), ('shall'), ('she'), ('should'), ('shouldn''t'), ('since'), ('six'), ('so'), ('some'), ('somebody'), ('somehow'), ('someone'), ('something'), ('sometime'), ('sometimes'), ('somewhat'), ('somewhere'), ('soon'), ('sorry'), ('specified'), ('specify'), ('specifying'), ('still'), ('sub'), ('such'), ('sup'), ('sure'), ('t'), ('t''s'), ('take'), ('taken'), ('tell'), ('tends'), ('th'), ('than'), ('thank'), ('thanks'), ('thanx'), ('that'), ('that''s'), ('thats'), ('the'), ('their'), ('theirs'), ('them'), ('themselves'), ('then'), ('thence'), ('there'), ('there''s'), ('thereafter'), ('thereby'), ('therefore'), ('therein'), ('theres'), ('thereupon'), ('these'), ('they'), ('they''d'), ('they''ll'), ('they''re'), ('they''ve'), ('think'), ('third'), ('this'), ('thorough'), ('thoroughly'), ('those'), ('though'), ('three'), ('through'), ('throughout'), ('thru'), ('thus'), ('to'), ('together'), ('too'), ('took'), ('toward'), ('towards'), ('tried'), ('tries'), ('truly'), ('try'), ('trying'), ('twice'), ('two'), ('u'), ('un'), ('under'), ('unfortunately'), ('unless'), ('unlikely'), ('until'), ('unto'), ('up'), ('upon'), ('us'), ('use'), ('used'), ('useful'), ('uses'), ('using'), ('usually'), ('uucp'), ('v'), ('value'), ('various'), ('very'), ('via'), ('viz'), ('vs'), ('w'), ('want'), ('wants'), ('was'), ('wasn''t'), ('way'), ('we'), ('we''d'), ('we''ll'), ('we''re'), ('we''ve'), ('welcome'), ('well'), ('went'), ('were'), ('weren''t'), ('what'), ('what''s'), ('whatever'), ('when'), ('whence'), ('whenever'), ('where'), ('where''s'), ('whereafter'), ('whereas'), ('whereby'), ('wherein'), ('whereupon'), ('wherever'), ('whether'), ('which'), ('while'), ('whither'), ('who'), ('who''s'), ('whoever'), ('whole'), ('whom'), ('whose'), ('why'), ('will'), ('willing'), ('wish'), ('with'), ('within'), ('without'), ('won''t'), ('wonder'), ('would'), ('would'), ('wouldn''t'), ('x'), ('y'), ('yes'), ('yet'), ('you'), ('you''d'), ('you''ll'), ('you''re'), ('you''ve'), ('your'), ('yours'), ('yourself'), ('yourselves'), ('z'), ('zero');
            """)
    else:
        con.sql(f"CREATE TABLE {fts_schema}.stopwords (sw VARCHAR);")

def create_duckdb_dict_table(con, fts_schema="fts_main_documents", stopwords='none'): 
    """
    Create the dict table using DuckDB's built-in dictionary functionality.
    """
    con.sql(f"DROP TABLE IF EXISTS {fts_schema}.dict;")
    create_stopwords_table(con, fts_schema, stopwords)
        
    con.sql(f"""
        CREATE TABLE {fts_schema}.dict AS
        WITH distinct_terms AS (
            SELECT DISTINCT term
            FROM {fts_schema}.terms
        )
        SELECT
            row_number() OVER () AS termid,
            term
        FROM
            distinct_terms
        {"WHERE term NOT IN (SELECT sw FROM " + fts_schema + ".stopwords)" if stopwords == 'english' else ''}
        ORDER BY term;
    """)

def build_dict_table(con, mode='duckdb', fts_schema="fts_main_documents", stopwords='none', gpt4_token_file=None, ngram_range=(1,2), min_freq=10, min_pmi=5.0):
    """
    Build the dictionary table using the specified mode.
    mode: 'phrases', 'ngrams', 'gpt4', or 'duckdb'
    """
    if mode == 'phrases':
        create_stopwords_table(con, fts_schema=fts_schema, stopwords=stopwords)
        extract_phrases_pmi_duckdb(con, fts_schema="fts_main_documents", n=2, min_freq=min_freq, min_pmi=min_pmi)
        print("Extracted phrases:", con.execute("SELECT * FROM fts_main_documents.phrases LIMIT 10").fetchall())

        print("\nAdded phrases to dictionary:", con.execute(f"SELECT * FROM {fts_schema}.dict LIMIT 10").fetchall())

        print("\nAdded tokens to dictionary:", con.execute(f"SELECT * FROM {fts_schema}.dict WHERE term NOT LIKE '% %' LIMIT 10").fetchall())
        con.execute(f"DROP TABLE IF EXISTS {fts_schema}.tokens")
        con.execute(f"DROP TABLE IF EXISTS {fts_schema}.phrases")
    elif mode == 'duckdb':
        create_terms_table_duckdb(con, fts_schema=fts_schema, input_schema="main", input_table="documents", input_id="did", input_val="content")
        create_duckdb_dict_table(con, fts_schema=fts_schema, stopwords=stopwords)
    else:
        raise ValueError(f"Unknown dict table build mode: {mode}")

def create_terms_table(con, fts_schema="fts_main_documents", input_schema="main", input_table="documents", input_id="did", input_val="content"):
    """
    Create the terms table with unique terms per docid.
    Assumes the table fts_main_documents.dict already exists.
    Adds a fieldid and termid column for compatibility with fielded search macros.
    """
    # Cleanup input text using the same regex as DuckDB's tokenizer
    con.sql(f"""
        CREATE OR REPLACE TABLE {fts_schema}.cleaned_docs AS
        SELECT
            {input_id},
            regexp_replace(lower(strip_accents(CAST({input_val} AS VARCHAR))),
                '[0-9!@#$%^&*()_+={{}}\\[\\]:;<>,.?~\\\\/\\|''''"`-]+', ' ', 'g') AS content,
        FROM {input_schema}.{input_table}
    """)

    # Use the ciff tokenizer to find bigrams and unigrams
    con.sql(f"""
        CREATE OR REPLACE TABLE {fts_schema}.terms AS (
            SELECT
                0 AS fieldid,
                d.termid,
                t.docid
            FROM (
                SELECT
                    row_number() OVER () AS docid,
                    unnest({fts_schema}.tokenize(content)) AS term
                FROM {fts_schema}.cleaned_docs
            ) AS t
            JOIN {fts_schema}.dict d ON t.term = d.term
            WHERE t.term != ''
        );
    """)


def create_terms_table_duckdb(con, fts_schema="fts_main_documents", input_schema="main", input_table="documents", input_id="did", input_val="content"):
    """
    Step 1: Create the initial terms table (term, docid).
    """
    con.sql(f"""
        CREATE OR REPLACE TABLE {fts_schema}.terms AS (
            SELECT
                row_number() OVER () AS docid,
                unnest({fts_schema}.tokenize({input_val})) AS term
            FROM {input_schema}.{input_table}
            WHERE {input_val} != ''
        );
    """)

def assign_termids_to_terms(con, fts_schema="fts_main_documents"):
    """
    Step 3: Recreate the terms table, joining with dict to assign termid.
    """
    con.sql(f"""
        CREATE OR REPLACE TABLE {fts_schema}.terms AS (
            SELECT
                0 AS fieldid,
                d.termid,
                t.docid,
                t.term,
                row_number() OVER (PARTITION BY t.docid) AS pos
            FROM {fts_schema}.terms t
            JOIN {fts_schema}.dict d ON t.term = d.term
            WHERE t.term != ''
        );
    """)

def update_docs_table(con, fts_schema="fts_main_documents"):
    """
    Create the documents table.
    input_id should be the column name in input_table that uniquely identifies each document (e.g., 'did').
    """
    # Remove old 'len' column if it exists, then add and populate a fresh one
    con.sql(f"ALTER TABLE {fts_schema}.docs DROP COLUMN IF EXISTS len;")
    con.sql(f"ALTER TABLE {fts_schema}.docs ADD COLUMN len INT;")
    con.sql(f"""
        UPDATE {fts_schema}.docs d
        SET len = (
            SELECT COUNT(termid)
            FROM {fts_schema}.terms t
            WHERE t.docid = d.docid
        );
    """)

def update_dict_table(con, fts_schema="fts_main_documents"):
    """
    Update the dictionary table with document frequency (df).
    Assumes the table fts_main_documents.dict already exists.
    """
    con.sql(f"ALTER TABLE {fts_schema}.dict ADD COLUMN IF NOT EXISTS df BIGINT;")
    con.sql(f"""
        UPDATE {fts_schema}.dict d
        SET df = (
            SELECT count(DISTINCT docid)
            FROM {fts_schema}.terms t
            WHERE t.termid = d.termid
        );
    """)

def limit_dict_table(con, max_terms=10000, fts_schema="fts_main_documents"):
    # Create a temporary table with limited terms and reassigned termid
    con.sql(f"""
        CREATE OR REPLACE TEMP TABLE temp_limited_dict AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY df DESC, term ASC) AS termid,
            term,
            df
        FROM {fts_schema}.dict
        ORDER BY df DESC, term ASC
        LIMIT {max_terms};
    """)

    # Drop original dict table
    con.sql(f"DROP TABLE IF EXISTS {fts_schema}.dict;")

    # Recreate dict table from temp table
    con.sql(f"""
        CREATE TABLE {fts_schema}.dict AS
        SELECT * FROM temp_limited_dict;
    """)

    # Drop temp table
    con.sql("DROP TABLE IF EXISTS temp_limited_dict;")



def create_stats_table(con, fts_schema="fts_main_documents", index_type="standard", stemmer="none"):
    """
    Create the stats table.
    This table contains statistics about the FTS index.
    Columns: num_docs, avgdl, sumdf, index_type, stemmer
    """
    con.sql(f"DROP TABLE IF EXISTS {fts_schema}.stats;")
    con.sql(f"""
        CREATE TABLE {fts_schema}.stats AS (
            SELECT 
                COUNT(docs.docid) AS num_docs,
                SUM(docs.len) / COUNT(docs.len) AS avgdl,
                (SELECT SUM(df) FROM fts_main_documents.dict) AS sumdf,
                '{index_type}' AS index_type,
                '{stemmer}' AS stemmer
            FROM {fts_schema}.docs AS docs
        );
    """)

def create_fields_table(con, fts_schema="fts_main_documents"):
    con.sql(f'''
        CREATE TABLE IF NOT EXISTS {fts_schema}.fields (
            fieldid INTEGER,
            field TEXT
        );
    ''')
    # Insert a default field if table is empty
    con.sql(f'''
        INSERT INTO {fts_schema}.fields (fieldid, field)
        SELECT 0, 'content'
        WHERE NOT EXISTS (SELECT 1 FROM {fts_schema}.fields);
    ''')

def index_documents(db_name, ir_dataset, stemmer='none', stopwords='none',
                     logging=True, keepcontent=False, limit=10000, mode='duckdb', min_freq=10, min_pmi=5.0):
    """
    Insert and index documents.
    """
    if pathlib.Path(db_name).is_file():
        raise ValueError(f"File {db_name} already exists.")
    con = duckdb.connect(db_name)
    insert_dataset(con, ir_dataset, logging)
    if logging:
        print("Indexing...", file=sys.stderr)

    docs = con.sql("SELECT * FROM documents LIMIT 10").df()
    print("Docs:\n", docs)

    create_docs_table(con, input_schema="main", input_table="documents", input_id="did")
    
    fts_docs = con.sql("SELECT * FROM fts_main_documents.docs LIMIT 10").df()
    print("fts_main_documents.docs:\n", fts_docs)

    con.sql("CREATE SCHEMA IF NOT EXISTS fts_main_documents;")
    con.sql("CREATE TABLE IF NOT EXISTS fts_main_documents.dict (term TEXT);")

    create_tokenizer_duckdb(con)

    # Create the dict table
    build_dict_table(con, mode=mode, fts_schema="fts_main_documents", stopwords=stopwords, ngram_range=(1,2), min_freq=min_freq, min_pmi=min_pmi)

    create_tokenizer_ciff(con)

    dict = con.sql("SELECT * FROM fts_main_documents.dict LIMIT 10").df()
    print("fts_main_documents.dict:\n", dict)

    # Clean up the terms table
    if mode == 'phrases':
        con.sql("DROP TABLE IF EXISTS fts_main_documents.terms;")
        create_terms_table(con, input_schema="main", input_table="documents", input_id="did", input_val="content")
    else:
        assign_termids_to_terms(con, fts_schema="fts_main_documents")

    terms = con.sql("SELECT * FROM fts_main_documents.terms LIMIT 10").df()
    print("fts_main_documents.terms:\n", terms)

    update_docs_table(con, fts_schema="fts_main_documents")

    docs = con.sql("SELECT * FROM fts_main_documents.docs LIMIT 10").df()
    print("fts_main_documents.docs:\n", docs)

    update_dict_table(con, fts_schema="fts_main_documents")
    print("Updated fts_main_documents.dict with document frequencies.")


    # Limit the dictionary to the `max_terms` most frequent terms
    if limit > 0:
        limit_dict_table(con, max_terms=limit, fts_schema="fts_main_documents")
        create_terms_table(con, fts_schema="fts_main_documents", input_schema="main", input_table="documents", input_id="did", input_val="content")
        update_dict_table(con, fts_schema="fts_main_documents")
        print("Limited fts_main_documents.dict to 10000 most frequent terms.")

    update_docs_table(con, fts_schema="fts_main_documents")

    dict = con.sql("SELECT * FROM fts_main_documents.dict LIMIT 10").df()
    print("fts_main_documents.dict:\n", dict)

    # Remove unused words from dictionary
    con.sql('''
        DELETE FROM fts_main_documents.dict
        WHERE df == 0;
    ''')

    create_stats_table(con, fts_schema="fts_main_documents", index_type="standard", stemmer=stemmer)

    stats = con.sql("SELECT * FROM fts_main_documents.stats").df()
    print("fts_main_documents.stats:\n", stats)

    create_fields_table(con, fts_schema="fts_main_documents")
    create_lm(con, stemmer)
    con.close()



if __name__ == "__main__":
    import argparse
    import ze_eval
    import os

    parser = argparse.ArgumentParser(description="Manual index builder for IR datasets.")
    parser.add_argument('--db', type=str, default='testje_docs.db', help='Database file name')
    parser.add_argument('--dataset', type=str, default='cranfield', help='ir_datasets name (e.g., cranfield, msmarco-passage)')
    parser.add_argument('--stemmer', type=str, default='none', help='Stemmer to use (none, porter, etc.)')
    parser.add_argument('--stopwords', type=str, default='english', help='Stopwords to use (english, none)')
    parser.add_argument('--mode', type=str, default='duckdb', help='Indexing mode (duckdb, ngrams, phrases, gpt4)')
    parser.add_argument('--keepcontent', action='store_true', help='Keep document content')
    parser.add_argument('--limit', type=int, default=10000, help='Limit the number of terms in the dictionary')
    parser.add_argument('--min-freq', type=int, default=10, help='Minimum frequency for phrases (only for mode "phrases")')
    parser.add_argument('--min-pmi', type=float, default=5.0, help='Minimum PMI for phrases (only for mode "phrases")')
    args = parser.parse_args()

    dataset = None
    if (args.dataset == 'custom'):
        dataset = ze_eval.ir_dataset_test()
    else:
        dataset = ir_datasets.load(args.dataset)
    db_name = args.db
    if os.path.exists(db_name):
        print(f"Removing {db_name}")
        os.remove(db_name)

    print("Creating index...")
    index_documents(
        db_name,
        dataset,
        stemmer=args.stemmer,
        stopwords=args.stopwords,
        keepcontent=args.keepcontent,
        mode=args.mode,
        limit=args.limit,
        min_freq=args.min_freq,
        min_pmi=args.min_pmi
    )
    print("")