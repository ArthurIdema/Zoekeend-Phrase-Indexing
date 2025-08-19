import pathlib
import sys

import duckdb
import ir_datasets


def copy_file(name_in, name_out):
    """ Simple file copy """
    path1 = pathlib.Path(name_in)
    if not path1.is_file():
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    if path2.is_file():
        raise ValueError(f"File {name_out} already exists.")
    path2.write_bytes(path1.read_bytes())


def get_stats_stemmer(con):
    """ What stemmer was used on this index? """
    sql = "SELECT stemmer FROM fts_main_documents.stats"
    return con.sql(sql).fetchall()[0][0]


def sample_by_values(con, column, threshold):
    """ Takes one sample per unique value of len/prior. """
    con.sql(f"""
        CREATE VIEW sample AS
        WITH histogram as (
          SELECT "{column}", COUNT(*) AS count
          FROM fts_main_documents.docs
          WHERE "{column}" > {threshold}
          GROUP BY "{column}"
        ) 
        SELECT LN(SUM(H2.count)) AS x, LN(H1."{column}") AS y
        FROM histogram H1, histogram H2
        WHERE H1."{column}" <= H2."{column}"
        GROUP BY H1."{column}"
    """)


def sample_by_fixed_points(con, column, threshold, total):
    """ Takes {total} samples and averages len/prior for each. """
    con.sql(f"""
        CREATE VIEW sample AS
        WITH groups AS (
          SELECT (CASE WHEN range = 2 THEN 0 ELSE range END) * 
            LN(num_docs + 1) / ({total} + 2) AS group_start,
            (range + 1) * LN(num_docs + 1) / ({total} + 2) AS group_end
          FROM RANGE({total} + 2), fts_main_documents.stats
          WHERE range > 1
        )
        SELECT (group_start + group_end) / 2 AS X, LN(AVG({column})) AS Y
        FROM groups, fts_main_documents.docs AS docs
        WHERE LN(docid + 1) >= group_start AND LN(docid + 1) < group_end
        AND "{column}" > {threshold}
        GROUP BY group_start, group_end
    """)


def sample_by_fixed_points_qrels(con, total):
    """
    Takes {total} samples and estimates the probability of relevance
    from the provided qrels
    """
    con.sql(f"""
        CREATE VIEW sample AS
        WITH groups AS (
          SELECT (CASE WHEN range = 2 THEN 0 ELSE range END) *
            LN(num_docs + 1) / ({total} + 2) AS group_start,
            (range + 1) * LN(num_docs + 1) / ({total} + 2) AS group_end
          FROM RANGE({total} + 2), fts_main_documents.stats
          WHERE range > 1
        )
        SELECT (group_start + group_end) / 2 AS X,
               LN(COUNT(*)/(EXP(group_end) - EXP(group_start))) AS Y
        FROM groups, fts_main_documents.docs AS docs, qrels
        WHERE LN(docid + 1) >= group_start AND LN(docid + 1) < group_end
        AND docs.name = qrels.did
        AND qrels.rel > 0
        GROUP BY group_start, group_end
    """)


def print_sample_tsv(con, total=None):
    """ Prints sample for drawing nice graphs. """
    result = con.sql("SELECT x, y FROM sample ORDER BY x").fetchall()
    if total and len(result) != total:
        print(f"Warning: less than {total} datapoints.", file=sys.stderr)
    for (x, y) in result:
        print(str(x) + "\t" + str(y))


def train_linear_regression(con):
    """ Approximate sample by using linear regression. """
    con.sql("""
        WITH sums AS (
          SELECT COUNT(*) AS N, SUM(x) AS Sx, SUM(y) AS Sy, 
            SUM(x*x) AS Sxx, SUM(x*y) AS Sxy
            FROM sample
        ),
        model AS (
            SELECT (Sy*Sxx - Sx*Sxy) / (N*Sxx - Sx*Sx) AS intercept,
                   (N*Sxy - Sx*Sy) / (N*Sxx - Sx*Sx) AS slope      
            FROM sums
        )
        UPDATE fts_main_documents.stats AS stats
        SET intercept = model.intercept, slope =
          CASE WHEN model.slope < 0 THEN model.slope ELSE 0 END
        FROM model
    """)


def get_qrels_from_file(qrel_file):
    inserts = []
    with open(qrel_file, "r", encoding="ascii") as file:
        for line in file:
            (query_id, q0 ,doc_id, relevance) = line.split()
            if relevance != 0:
                inserts.append([query_id, doc_id, relevance])
    return inserts


def get_qrels_from_ir_datasets(qrels_tag):
    inserts = []
    for q in ir_datasets.load(qrels_tag).qrels_iter():
        if q.relevance != 0:
            inserts.append([q.query_id, q.doc_id, q.relevance])
    return inserts


def insert_qrels(con, qrels_tag):
    con.sql("CREATE OR REPLACE TABLE main.qrels(qid TEXT, did TEXT, rel INT)")
    try:
        inserts = get_qrels_from_ir_datasets(qrels_tag)
    except KeyError:
        inserts = get_qrels_from_file(qrels_tag)
    con.sql("BEGIN TRANSACTION")
    con.executemany("INSERT INTO qrels VALUES (?, ?, ?)", inserts)
    con.sql("COMMIT")


def replace_bm25_fitted_doclen(con, stemmer):
    con.sql(f"""
        CREATE OR REPLACE MACRO fts_main_documents.match_bm25(docname, query_string, b := 0.75, k := 1.2, conjunctive := 0, fields := NULL) AS (
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
            SELECT docs.docid, EXP(LN(docs.docid)*stats.slope + stats.intercept) AS newlen, term_tf.termid, tf, df, (log((((stats.num_docs - df) + 0.5) / (df + 0.5))) * ((tf * (k + 1)) / (tf + (k * ((1 - b) + (b * (newlen / stats.avgdl))))))) AS subscore
            FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids,
                fts_main_documents.stats AS stats,
                WHERE term_tf.docid = cdocs.docid
                AND term_tf.docid = docs.docid
                AND term_tf.termid = qtermids.termid
            ),
            scores AS (
                SELECT docid, sum(subscore) AS score
                FROM subscores
                GROUP BY docid
            )
            SELECT score
            FROM scores, fts_main_documents.docs AS docs
            WHERE scores.docid = docs.docid
              AND docs.name = docname
        )"""
    )


def replace_lm_fitted_doclen(con, stemmer):
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
                SELECT docs.docid, EXP(LN(docs.docid)*stats.slope + stats.intercept) AS newlen,
                  term_tf.termid, tf, df,
                  LN(1 + (lambda * tf * (SELECT sumdf FROM fts_main_documents.stats)) / ((1-lambda) * df * newlen)) AS subscore
                FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids, 
                     fts_main_documents.stats AS stats
                WHERE term_tf.docid = cdocs.docid
                AND term_tf.docid = docs.docid
                AND term_tf.termid = qtermids.termid
            ),
            scores AS (
                SELECT docid, LN(ANY_VALUE(newlen)) + sum(subscore) AS score
                FROM subscores
                GROUP BY docid
            )
            SELECT score
            FROM scores, fts_main_documents.docs AS docs
            WHERE scores.docid = docs.docid
              AND docs.name = docname
        )"""
    )


def replace_lm_fitted_prior(con, stemmer='none'):
    """
    Only use fitted prior, but keep on using the old document lengths.
    """
    sql = f"""
        CREATE OR REPLACE MACRO fts_main_documents.match_lm(docname, query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS (
            WITH tokens AS (
                SELECT stem(unnest(fts_main_documents.tokenize(query_string)), '{stemmer}') AS t
            ),
            fieldids AS (
                SELECT fieldid
                FROM fts_main_documents.fields
                WHERE CASE WHEN fields IS NULL THEN 1 ELSE field IN (SELECT * FROM (SELECT UNNEST(string_split(fields, ','))) AS fsq) END
            ),
            qtermids AS (
                SELECT termid, df, COUNT(*) AS qtf
                FROM fts_main_documents.dict AS dict,
                     tokens
                WHERE dict.term = tokens.t
                GROUP BY termid, df
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
                SELECT docs.docid, docs.len, term_tf.termid, term_tf.tf, qtermids.df,
                    qtermids.qtf * LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * len)) AS subscore
                FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
                WHERE term_tf.docid = cdocs.docid
                AND term_tf.docid = docs.docid
                AND term_tf.termid = qtermids.termid
            ),
            scores AS (
                SELECT docid, (LN(docid)*(SELECT ANY_VALUE(slope) FROM fts_main_documents.stats)) + sum(subscore) AS score
                FROM subscores
                GROUP BY docid
            )
            SELECT score
            FROM scores, fts_main_documents.docs AS docs
            WHERE scores.docid = docs.docid
              AND docs.name = docname
        )
        """
    con.sql(sql)


def renumber_doc_ids(con, column):
    con.sql(f"""
        -- renumber document ids by decreasing len/prior column
        CREATE TABLE fts_main_documents.docs_new AS
        SELECT ROW_NUMBER() over (ORDER BY "{column}" DESC, name ASC) newid, docs.*
        FROM fts_main_documents.docs AS docs;
        -- update postings
        CREATE TABLE fts_main_documents.terms_new AS
        SELECT D.newid as docid, T.fieldid, T.termid
        FROM fts_main_documents.terms T, fts_main_documents.docs_new D
        WHERE T.docid = D.docid 
        ORDER BY T.termid;
        -- replace old by new data
        ALTER TABLE fts_main_documents.docs_new DROP COLUMN docid;
        ALTER TABLE fts_main_documents.docs_new RENAME COLUMN newid TO docid;
        DROP TABLE fts_main_documents.docs;
        DROP TABLE fts_main_documents.terms;
        ALTER TABLE fts_main_documents.docs_new RENAME TO docs;
        ALTER TABLE fts_main_documents.terms_new RENAME TO terms;
        UPDATE fts_main_documents.stats SET index_type = 'fitted';
    """)


def reindex_fitted_column(name_in, name_out, column='prior', total=None,
                          print_sample=False, threshold=0, qrels=None):
    if column not in ['len', 'prior']:
        raise ValueError(f'Column "{column}" not allowed: use len or prior.')
    copy_file(name_in, name_out)
    con = duckdb.connect(name_out)
    renumber_doc_ids(con, column)
    try:
        con.sql("""
            ALTER TABLE fts_main_documents.stats ADD intercept DOUBLE;
            ALTER TABLE fts_main_documents.stats ADD slope DOUBLE;
        """)
    except duckdb.duckdb.CatalogException as e:
        print ("Warning: " + str(e), file=sys.stderr)
    if qrels:
        insert_qrels(con, qrels)
        if total:
            sample_by_fixed_points_qrels(con, total)
        else:
            raise ValueError("Not implemented.")
    else:
        if total:
            sample_by_fixed_points(con, column, threshold, total)
        else:
            sample_by_values(con, column, threshold)
    if print_sample:
        print_sample_tsv(con, total)
    train_linear_regression(con)
    con.sql(f"""
        DROP VIEW sample;
        ALTER TABLE fts_main_documents.docs DROP COLUMN "{column}";
    """)
    stemmer = get_stats_stemmer(con)
    if column == 'len':
        replace_lm_fitted_doclen(con, stemmer=stemmer)
        replace_bm25_fitted_doclen(con, stemmer=stemmer)
    else:
        replace_lm_fitted_prior(con, stemmer=stemmer)
    con.close()


if __name__ == "__main__":
    reindex_fitted_column('robustZE.db', 'robustZE_fitted20.db', column='len', total=None, print_sample=True, threshold=20, qrels=None)
