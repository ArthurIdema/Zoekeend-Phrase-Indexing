import pathlib
import sys

import duckdb


def copy_file(name_in, name_out):
    path1 = pathlib.Path(name_in)
    if not path1.is_file():
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    if path2.is_file():
        raise ValueError(f"File {name_out} already exists.")
    path2.write_bytes(path1.read_bytes())


def get_stats_stemmer(con):
    sql = "SELECT stemmer FROM fts_main_documents.stats"
    return con.sql(sql).fetchall()[0][0]


def replace_lm_prior(con, stemmer):
    con.sql(f"""
        CREATE OR REPLACE MACRO fts_main_documents.match_lm(docname, query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS (
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
           SELECT docs.docid, prior, len, term_tf.termid, tf, df, LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * len)) AS subscore
           FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
           WHERE ((term_tf.docid = cdocs.docid)
           AND (term_tf.docid = docs.docid)
           AND (term_tf.termid = qtermids.termid))
        ),
        scores AS (
           SELECT docid, LN(ANY_VALUE(prior)) + sum(subscore) AS score FROM subscores GROUP BY docid
        )
        SELECT score FROM scores, fts_main_documents.docs AS docs
        WHERE ((scores.docid = docs.docid) AND (docs."name" = docname)))
    """)


def insert_priors(con, csv_file, default):
    con.sql(f"""
        UPDATE fts_main_documents.docs AS docs
        SET prior = priors.prior
        FROM read_csv({csv_file}) AS priors
        WHERE docs.name = priors.did
    """)
    if not default is None:
        con.sql(f"""
            UPDATE fts_main_documents.docs
            SET prior = {default}
            WHERE prior IS NULL
        """)
    else:
        count = con.sql("""
            SELECT COUNT(*)
            FROM fts_main_documents.docs
            WHERE prior IS NULL
        """).fetchall()[0][0]
        if count > 0:
            print(f"Warning: {count} rows missing from file. Use --default", file=sys.stderr)


def reindex_prior(name_in, name_out, csv_file=None, default=None, init=None):
    copy_file(name_in, name_out)
    con = duckdb.connect(name_out)
    con.sql("ALTER TABLE fts_main_documents.docs ADD prior DOUBLE")
    if (csv_file and init):
        print(f"Warning: init={init} ignored.", file=sys.stderr)
    if csv_file:
        insert_priors(con, csv_file, default)
    elif init:
        if init == 'len':
            con.sql("UPDATE fts_main_documents.docs SET prior = len")
        elif init == 'uniform':
            con.sql("UPDATE fts_main_documents.docs SET prior = 1")
        else:
            raise ValueError(f'Unknown value for init: {init}')
    stemmer = get_stats_stemmer(con)
    replace_lm_prior(con, stemmer=stemmer)
    con.close()


if __name__ == "__main__":
    reindex_prior('cran.db', 'cran_prior.db', csv_file='test_priors.csv')
