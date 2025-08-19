import duckdb
import pathlib
import sys


def copy_file(name_in, name_out):
    path1 = pathlib.Path(name_in)
    if not(path1.is_file()):
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    if path2.is_file():
        raise ValueError(f"File {name_out} already exists.")
    path2.write_bytes(path1.read_bytes())


def get_stats_stemmer(con):
    sql = "SELECT stemmer FROM fts_main_documents.stats"
    return con.sql(sql).fetchall()[0][0]


def replace_bm25(con, stemmer):
    """ The standard DuckDB BM25 implementation does not work with the grouped index.
        This version also works with the standard DuckDB index.
    """
    con.sql(f"""
      CREATE OR REPLACE MACRO fts_main_documents.match_bm25(docname, query_string, b := 0.75, k := 1.2, conjunctive := 0, fields := NULL) AS (
        WITH tokens AS (
          SELECT DISTINCT stem(unnest(fts_main_documents.tokenize(query_string)), '{stemmer}') AS t
        ),
        fieldids AS (
          SELECT fieldid
          FROM fts_main_documents.fields
          WHERE CASE  WHEN ((fields IS NULL)) THEN (1) ELSE (field = ANY(SELECT * FROM (SELECT unnest(string_split(fields, ','))) AS fsq)) END
        ),
        qtermids AS (
          SELECT termid, df
          FROM fts_main_documents.dict AS dict, tokens
          WHERE (dict.term = tokens.t)
        ),
        qterms AS (
          SELECT termid, docid
          FROM fts_main_documents.terms AS terms
          WHERE (CASE  WHEN ((fields IS NULL)) THEN (1) ELSE (fieldid = ANY(SELECT * FROM fieldids)) END
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
          SELECT docs.docid, len, term_tf.termid, tf, df,
            (log((((((SELECT num_docs FROM fts_main_documents.stats) - df) + 0.5) / (df + 0.5)) + 1)) * ((tf * (k + 1)) / (tf + (k * ((1 - b) + (b * (len / (SELECT avgdl FROM fts_main_documents.stats)))))))) AS subscore
          FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
          WHERE (term_tf.docid = cdocs.docid)
          AND (term_tf.docid = docs.docid)
          AND (term_tf.termid = qtermids.termid)
        ),
        scores AS (
          SELECT docid, sum(subscore) AS score
          FROM subscores
          GROUP BY docid
        )
        SELECT score
        FROM scores, fts_main_documents.docs AS docs
        WHERE (scores.docid = docs.docid) AND (docs."name" = docname)
      )
    """)


def reindex_group(name_in, name_out, stemmer='porter'):
    copy_file(name_in, name_out)
    con = duckdb.connect(name_out)
    oldstemmer = get_stats_stemmer(con)
    if oldstemmer != 'none':
        print(f"Warning: stemmer {oldstemmer} was already used on this database")
    con.sql(f"""
        -- newdict gives stems unique ids
        CREATE TABLE fts_main_documents.newdict AS
        SELECT termid, term, stem(term, '{stemmer}') AS stem, DENSE_RANK() OVER (ORDER BY stem) AS newid, df
        FROM fts_main_documents.dict;
        DROP TABLE fts_main_documents.dict;
        -- newterms uses those new ids
        CREATE TABLE fts_main_documents.newterms AS
        SELECT terms.docid, terms.fieldid, newdict.newid AS termid
        FROM fts_main_documents.terms AS terms, fts_main_documents.newdict AS newdict
        WHERE terms.termid = newdict.termid;
        DROP TABLE fts_main_documents.terms;
        ALTER TABLE fts_main_documents.newterms RENAME TO terms;
        -- now remove old ids from dict table and compute new dfs.
        CREATE TABLE fts_main_documents.dict AS
        SELECT D.newid AS termid, D.term, COUNT(DISTINCT T.docid) AS df
        FROM fts_main_documents.newdict D, fts_main_documents.terms T
        WHERE T.termid = D.newid
        GROUP BY D.newid, D.term;
        DROP TABLE fts_main_documents.newdict;
        -- update stats
        UPDATE fts_main_documents.stats SET index_type = 'grouped({stemmer})';
    """)
    replace_bm25(con, oldstemmer)
    con.close()


if __name__ == "__main__":
    reindex_group('robustZE.db', 'robustZEgrouped.db')

