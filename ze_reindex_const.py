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


def replace_bm25_const(con, stemmer):
    """ New version of BM25; assuming that const_len=avgdl, the document
        length normalization part disappears and the ranking function
        becomes BM1 from Robertson and Walker's SIGIR 1994 paper.
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
          SELECT docs.docid, term_tf.termid, tf, df,
            (log((((((SELECT num_docs FROM fts_main_documents.stats) - df) + 0.5) / (df + 0.5)) + 1)) * ((tf * (k + 1)) / (tf + k))) AS subscore
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


def get_sql_selects(con):
    try:
        con.sql('SELECT prior FROM fts_main_documents.docs')
    except duckdb.duckdb.BinderException:
        pass
    else: # there is a prior column (from reindex_prior)
        return ("docs.prior,", "LN(ANY_VALUE(prior)) +")
    try:
        con.sql('SELECT slope FROM fts_main_documents.stats')
    except duckdb.duckdb.BinderException:
        pass
    else: # there is a slope column (from reindex_fitted)
        return ("", "(LN(docid)*(SELECT ANY_VALUE(slope) FROM fts_main_documents.stats)) +")
    return ("", "")


def replace_lm_const(con, stemmer, const_len):
    """ This is a language model matcher where len is replaced by a constant.
        It uses the prior column or fitted score, if present in the old index.
    """
    (subscores_select, scores_select) = get_sql_selects(con) # adapt to previous index
    con.sql(f"""
        CREATE OR REPLACE MACRO fts_main_documents.match_lm(docname, query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS (
            WITH tokens AS (
                SELECT DISTINCT stem(unnest(fts_main_documents.tokenize(query_string)), '{stemmer}') AS t
            ),
            fieldids AS (
                SELECT fieldid
                FROM fts_main_documents.fields
                WHERE CASE WHEN fields IS NULL THEN 1 ELSE field IN (SELECT * FROM (SELECT UNNEST(string_split(fields, ','))) AS fsq) END
            ),
            qtermids AS (
                SELECT termid, df
                FROM fts_main_documents.dict AS dict,
                     tokens
                WHERE dict.term = tokens.t
            ),
            qterms AS (
                SELECT termid,
                       docid
                FROM fts_main_documents.terms AS terms
                WHERE CASE WHEN fields IS NULL THEN 1 ELSE fieldid IN (SELECT * FROM fieldids) END
                  AND termid IN (SELECT qtermids.termid FROM qtermids)
            ),
            term_tf AS (
                SELECT termid, docid, COUNT(*) AS tf
                FROM qterms
                GROUP BY docid, termid
            ),
            cdocs AS (
                SELECT docid
                FROM qterms
                GROUP BY docid
                HAVING CASE WHEN conjunctive THEN COUNT(DISTINCT termid) = (SELECT COUNT(*) FROM tokens) ELSE 1 END
            ),
           subscores AS (
                SELECT {subscores_select} docs.docid, term_tf.termid, term_tf.tf, qtermids.df,
                    LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * (SELECT ANY_VALUE(const_len) FROM fts_main_documents.stats))) AS subscore
                FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
                WHERE term_tf.docid = cdocs.docid
                AND term_tf.docid = docs.docid
                AND term_tf.termid = qtermids.termid
            ),
            scores AS (
                SELECT docid, {scores_select} sum(subscore) AS score
                FROM subscores
                GROUP BY docid
            )
            SELECT score
            FROM scores, fts_main_documents.docs AS docs
            WHERE scores.docid = docs.docid
              AND docs.name = docname
        )
        """)


def reindex_const(name_in, name_out, const_len=400, b=1, keep_terms=False, maxp=1.0):
    copy_file(name_in, name_out)
    con = duckdb.connect(name_out)
    max_tf = int(const_len * maxp)
    if keep_terms:
        new_tf = 'CASE WHEN tf > 0.5 THEN tf - 0.5 ELSE 0.1 END'
    else:
        new_tf = 'tf - 0.5'
    con.sql(f"""
        CREATE TABLE fts_main_documents.terms_new (
          docid BIGINT, fieldid BIGINT, termid BIGINT);
        WITH sequence AS (
          SELECT range AS nr FROM RANGE({max_tf})
        ),
        tf_new AS (
          SELECT T.docid, T.fieldid, termid, 
          -- BM25-like length normalization:
          COUNT(*) / (1 - {b} + {b} * (ANY_VALUE(D.len) / {const_len})) AS tf,
          -- proper rounding, but do not remove terms:
          {new_tf} AS new_tf
          FROM fts_main_documents.terms T, fts_main_documents.docs D 
          WHERE T.docid = D.docid 
          GROUP BY T.docid, T.fieldid, T.termid
        ) 
        INSERT INTO fts_main_documents.terms_new 
        SELECT docid, fieldid, termid 
        FROM tf_new, sequence WHERE sequence.nr < tf_new.new_tf;
        DROP TABLE fts_main_documents.terms;
        ALTER TABLE fts_main_documents.terms_new RENAME TO terms;
        UPDATE fts_main_documents.stats 
          SET index_type = 'const(len={const_len},b={b})';
        ALTER TABLE fts_main_documents.stats ADD const_len BIGINT;
        UPDATE fts_main_documents.stats SET const_len = {const_len};
        -- really remove len column
        ALTER TABLE fts_main_documents.docs DROP COLUMN len;
    """)
    stemmer = get_stats_stemmer(con)
    replace_bm25_const(con, stemmer)
    replace_lm_const(con, stemmer, const_len)
    con.close()


if __name__ == "__main__":
    reindex_const('robustZE.db', 'robustZEfitted01.db', const_len=500, maxp=0.1)

