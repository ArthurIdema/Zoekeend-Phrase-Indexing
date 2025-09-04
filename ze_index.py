"""
Zoekeend indexer.
Author: Djoerd Hiemstra
"""

import pathlib
import sys

import duckdb
import ir_datasets


def normalize(text):
    """ Escape quotes for SQL """
    return text.replace("'", "''")


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


def index_documents(db_name, ir_dataset, stemmer='none', stopwords='none',
                     logging=True, keepcontent=False):
    """
    Insert and index documents.
    """
    if pathlib.Path(db_name).is_file():
        raise ValueError(f"File {db_name} already exists.")
    con = duckdb.connect(db_name)
    insert_dataset(con, ir_dataset, logging)
    if logging:
        print("Indexing...", file=sys.stderr)
    con.sql(f"""
        PRAGMA create_fts_index('documents', 'did', 'content', stemmer='{stemmer}',
            stopwords='{stopwords}')
    """)
    con.sql(f"""
        ALTER TABLE fts_main_documents.stats ADD sumdf BIGINT;
        UPDATE fts_main_documents.stats SET sumdf =
            (SELECT SUM(df) FROM fts_main_documents.dict);
        ALTER TABLE fts_main_documents.stats ADD index_type TEXT; 
        UPDATE fts_main_documents.stats SET index_type = 'standard';
        ALTER TABLE fts_main_documents.stats ADD stemmer TEXT; 
        UPDATE fts_main_documents.stats SET stemmer = '{stemmer}';

    """)
    create_lm(con, stemmer)
    if not keepcontent:
        con.sql("ALTER TABLE documents DROP COLUMN content")
    con.close()


if __name__ == "__main__":
    import ze_eval
    dataset = ze_eval.ir_dataset_test()
    dataset = ir_datasets.load("cranfield")
    import os
    if os.path.exists('testje_docs.db'):
        os.remove('testje_docs.db')
    index_documents('testje_docs.db', dataset, stemmer='none', stopwords='none',
        keepcontent=False)
