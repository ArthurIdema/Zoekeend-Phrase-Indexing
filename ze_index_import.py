"""
CIFF importer

Author: Arjen P. de Vries

Adapted from: https://github.com/arjenpdevries/CIFF2DuckDB
"""

import duckdb
import pyarrow as pa

from ciff_toolkit.read import CiffReader
from ciff_toolkit.ciff_pb2 import DocRecord, Header, PostingsList
from google.protobuf.json_format import MessageToJson, MessageToDict
from typing import Iterator, TypeVar, Iterable

pbopt = {"including_default_value_fields": True,
         "preserving_proto_field_name": True}


def iter_posting_batches(reader: Iterable[PostingsList]):
    """
    Generator for reading batches of postings
    Note: Term identifiers handed out here, while reading term-posting
    pairs from the CIFF file
    """
    batch = []
    for tid, p in enumerate(reader.read_postings_lists()):
        pp = MessageToDict(p, **pbopt)
        pp['termid']=tid
        # Gap Decompression...
        pp['postings']=[prev := {"docid":0}] and \
            [prev := {"docid": posting['docid'] + prev['docid'], "tf": posting['tf']} for posting in pp['postings']]
        batch.append(pp)
        if len(batch) == 4096:
            yield pa.RecordBatch.from_pylist(batch)
            batch = []
    yield pa.RecordBatch.from_pylist(batch)


def iter_docs_batches(reader: Iterable[DocRecord]):
    """ Generator for reading batches of docs """
    batch = []
    for doc in reader.read_documents():
        batch.append(MessageToDict(doc, **pbopt))
        if len(batch) == 8192:
            yield pa.RecordBatch.from_pylist(batch)
            batch = []
    yield pa.RecordBatch.from_pylist(batch)


def ciff_arrow(con, file_name, stemmer):
    """ Use CIFFReader to create RecordBatches for table (using Arrow) """
    # Schema: manually defined
    # (alternative: protarrow could create the datastructure from the proto definition)
    postings_schema = pa.schema([
        ("term", pa.string()),
        ("termid", pa.int64()),
        ("df", pa.int64()),
        ("cf", pa.int64()),
        ("postings", pa.list_(pa.struct([
            ("docid", pa.int32()),
            ("tf", pa.int32())
            ])))
         ])

    docs_schema = pa.schema([
        ("docid", pa.int32()),
        ("collection_docid", pa.string()),
        ("doclength", pa.int32())
     ])

    with CiffReader(file_name) as reader:
        # Header info: TBD
        h = reader.read_header()
        header = MessageToJson(h, **pbopt)
        con.execute(f"""
            CREATE TABLE stats(num_docs BIGINT, avgdl DOUBLE, sumdf BIGINT, index_type TEXT, stemmer TEXT);
            INSERT INTO stats(num_docs, avgdl, index_type, stemmer) VALUES
              ({h.num_docs}, {h.average_doclength}, 'standard', '{stemmer}');
        """)

        # RecordBatches for postings to an Arrow Datastructure
        postings_rb = iter_posting_batches(reader)
        postings_rbr = pa.ipc.RecordBatchReader.from_batches(postings_schema, postings_rb)

        # Create a DuckDB table from the Arrow data
        con.execute("CREATE TABLE ciff_postings AS SELECT * FROM postings_rbr;")

        # RecordBatches for docs to an Arrow Datastructure
        docs_rb = iter_docs_batches(reader)
        docs_rbr = pa.ipc.RecordBatchReader.from_batches(docs_schema, docs_rb)

        # Create a DuckDB table from the Arrow data
        # Dropping cf here because DuckDB FTS does not use it
        con.execute("""
          CREATE TABLE docs AS SELECT docid::BIGINT AS docid, collection_docid AS name, doclength::BIGINT AS len FROM docs_rbr;
        """)


def create_tokenizer(con, tokenizer):
    if tokenizer == 'ciff':
        create_tokenizer_ciff(con)
    elif tokenizer == 'duckdb':
        create_tokenizer_duckdb(con)
    else:
        raise ValueError(f"Unknown tokenizer: {tokenizer}")


def create_tokenizer_duckdb(con):
    con.sql("""
        CREATE MACRO fts_main_documents.tokenize(s) AS (
            string_split_regex(regexp_replace(lower(strip_accents(CAST(s AS VARCHAR))), '[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\\\/\\|''''"`-]+', ' ', 'g'), '\\s+')
        );
    """)


def create_tokenizer_ciff(con):
    con.sql("""
        CREATE MACRO fts_main_documents.tokenize(query_string) AS (
          WITH RECURSIVE sequence AS (
            SELECT range AS nr 
            FROM RANGE((SELECT MAX(LEN(term)) + 1 FROM fts_main_documents.dict))
          ),
          simpledict AS (
            SELECT '' AS term
            UNION
            SELECT term FROM fts_main_documents.dict
          ),
          subterms(term, subquery) AS (
            SELECT '', lower(strip_accents(CAST(query_string AS VARCHAR)))
            UNION
            SELECT MAX(dict.term), SUBSTRING(subquery,
              -- MAX(dict.term) selects the longest term, for a
              -- start position using alphabetic sorting
              CASE WHEN MAX(nr) < 1 THEN 2 ELSE MAX(nr) + 1 END,
              LEN(subquery)) AS subquery
            FROM subterms, sequence, simpledict as dict
            WHERE SUBSTRING(subquery, 1, nr) = dict.term
            GROUP BY subquery
         )
         SELECT LIST(term)  FROM subterms WHERE NOT term = ''
       )
    """)


def create_lm(con, stemmer):
    con.sql(f"""
        CREATE MACRO fts_main_documents.match_lm(docname, query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS (
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
           SELECT docs.docid, docs.len, term_tf.termid, term_tf.tf, qtermids.df, LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * len)) AS subscore
           FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
           WHERE ((term_tf.docid = cdocs.docid)
           AND (term_tf.docid = docs.docid)
           AND (term_tf.termid = qtermids.termid))
        ),
        scores AS (
           SELECT docid, LN(MAX(len)) + sum(subscore) AS score FROM subscores GROUP BY docid
        )
        SELECT score FROM scores, fts_main_documents.docs AS docs
        WHERE ((scores.docid = docs.docid) AND (docs."name" = docname)))
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
        )
        SELECT score FROM scores, fts_main_documents.docs AS docs
        WHERE ((scores.docid = docs.docid) AND (docs."name" = docname)))
    """)


def ciff_import(db_name, file_name, tokenizer='ciff', stemmer='none'):
    con = duckdb.connect(db_name)
    con.execute("""
        CREATE SCHEMA fts_main_documents;
        USE fts_main_documents;
    """)
    ciff_arrow(con, file_name, stemmer)
    con.execute("""
        CREATE TABLE dict AS SELECT termid, term, df FROM ciff_postings;
        CREATE TABLE fts_main_documents.fields(fieldid BIGINT, field VARCHAR);
        CREATE TABLE terms(docid BIGINT, fieldid BIGINT, termid BIGINT);
        WITH postings AS (
          SELECT termid, unnest(postings, recursive := true) 
          FROM ciff_postings
        )
        INSERT INTO terms(docid, fieldid, termid)
        SELECT docid, 0, termid 
        FROM postings, range(tf)
        ORDER BY termid;
        DROP TABLE ciff_postings;
        CREATE TABLE main.documents AS SELECT DISTINCT name AS did FROM fts_main_documents.docs;
        -- new stats
        UPDATE fts_main_documents.stats SET sumdf = (SELECT SUM(df) FROM fts_main_documents.dict);
    """)
    create_tokenizer(con, tokenizer)
    create_lm(con, stemmer)
    create_bm25(con, stemmer)
    con.close()


if __name__ == "__main__":
    DB_NAME = "ciff-geesedb.db"
    FILE_NAME = "geesedb.ciff.gz"
    ciff_import(DB_NAME, FILE_NAME, tokenizer='ciff', stemmer='none')

    # Only for testing:
    # Query the index using the DuckDB tables

    connect = duckdb.connect(DB_NAME)
    connect.execute("USE fts_main_documents;")
    results = connect.execute("SELECT termid FROM dict WHERE term LIKE '%radboud%' OR term LIKE '%university%'").arrow()
    print(results)
    results = connect.execute("SELECT * FROM terms WHERE termid IN (select termid FROM dict WHERE term LIKE '%radboud%' OR term LIKE '%university%')").arrow()
    print(results)
