"""
Zoekeend CIFF exporter

Author: Gijs Hendriksen
"""

from typing import Iterable, Type, TypeVar
import duckdb

from ciff_toolkit.write import CiffWriter
from ciff_toolkit.ciff_pb2 import Header, PostingsList, DocRecord
from google.protobuf.message import Message

from tqdm import tqdm


M = TypeVar('M', bound=Message)


def _create_message_from_row(row: tuple | dict, message_type: Type[M]) -> M:
    if isinstance(row, tuple):
        mapping = zip(message_type.DESCRIPTOR.fields, row)
    else:
        mapping = [(field, row[field.name]) for field in message_type.DESCRIPTOR.fields]

    msg = message_type()
    for field, value in mapping:
        if field.label == field.LABEL_REPEATED:
            for x in value:
                getattr(msg, field.name).append(_create_message_from_row(x, field.message_type._concrete_class))
        else:
            setattr(msg, field.name, value)
    return msg


def create_protobuf_messages_from_result(result: duckdb.DuckDBPyRelation, message_type: Type[M], batch_size: int = 1024) -> Iterable[M]:
    try:
        import protarrow
        for batch in result.fetch_arrow_reader(batch_size):
            yield from protarrow.record_batch_to_messages(batch, message_type)
    except ImportError:
        while batch := result.fetchmany(batch_size):
            for row in batch:
                yield _create_message_from_row(row, message_type)


def create_ciff_header(conn: duckdb.DuckDBPyConnection, description: str) -> Header:
    header_info = conn.execute("""
        SELECT
            1 AS version,
            (SELECT COUNT(*) FROM fts_main_documents.dict) AS num_postings_lists,
            num_docs,
            (SELECT COUNT(*) FROM fts_main_documents.dict) AS total_postings_lists,
            num_docs AS total_docs,
            (SELECT SUM(len) FROM fts_main_documents.docs)::BIGINT AS total_terms_in_collection,
            avgdl AS average_doclength,
            ? AS description,
        FROM fts_main_documents.stats
    """, [description])

    header, = create_protobuf_messages_from_result(header_info, Header)
    return header


def create_ciff_postings_lists(conn: duckdb.DuckDBPyConnection, batch_size: int = 1024) -> Iterable[PostingsList]:
    postings_info = conn.sql("""
        WITH postings AS (
            SELECT termid, docid, COUNT(*) AS tf
            FROM fts_main_documents.terms
            GROUP BY ALL
        ),
        gapped_postings AS (
            SELECT *, docid - lag(docid, 1, 0) OVER (PARTITION BY termid ORDER BY docid) AS gap
            FROM postings
        ),
        grouped_postings AS (
            SELECT termid, list(row(gap, tf)::STRUCT(docid BIGINT, tf BIGINT) ORDER BY docid) AS postings, SUM(tf)::BIGINT AS cf
            FROM gapped_postings
            GROUP BY termid
        )
        SELECT term, df, cf, postings
        FROM grouped_postings
        JOIN fts_main_documents.dict USING (termid)
        ORDER BY term;
    """)

    yield from create_protobuf_messages_from_result(postings_info, PostingsList, batch_size=batch_size)


def create_ciff_doc_records(conn: duckdb.DuckDBPyConnection, batch_size: int = 1024) -> Iterable[DocRecord]:
    docs_info = conn.sql("""
        SELECT
            docid,
            name AS collection_docid,
            len AS doclength,
        FROM fts_main_documents.docs
        ORDER BY collection_docid
    """)

    yield from create_protobuf_messages_from_result(docs_info, DocRecord, batch_size=batch_size)


def ciff_export(db_name: str, file_name: str, description: str, batch_size: int = 1024):
    with duckdb.connect(db_name) as conn, CiffWriter(file_name) as writer:
        header = create_ciff_header(conn, description)
        print(header)
        writer.write_header(header)
        writer.write_postings_lists(tqdm(create_ciff_postings_lists(conn, batch_size=batch_size), total=header.num_postings_lists,
                                         desc='Writing posting lists', unit='pl'))
        writer.write_documents(tqdm(create_ciff_doc_records(conn, batch_size=batch_size), total=header.num_docs,
                                    desc='Writing documents', unit='d'))


if __name__ == '__main__':
    ciff_export('index.db', 'index-copy.ciff.gz', 'OWS.eu index', batch_size=2**12)
