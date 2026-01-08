"""
Zoekeend searcher.
Author: Djoerd Hiemstra
"""

import sys

import duckdb
import ir_datasets


def duckdb_search_lm(con, query, limit):
    sql = """
        SELECT docname, score, postings_cost
        FROM fts_main_documents.match_lm($1)
        ORDER BY score DESC
        LIMIT $2
    """
    return con.execute(sql, [query, limit]).fetchall()

# def duckdb_search_lm(con, query, limit, l):
#     print(f"Searching for: {query} with limit {limit} and l={l}")
#     sql = """
#         SELECT docname, score, postings_cost
#         FROM fts_main_documents.match_lm(docname, $1)
#         ORDER BY score DESC
#         LIMIT $2
#     """
#     return con.execute(sql, [query, limit]).fetchall()

def duckdb_print_query(con, query):
    sql = """
        WITH stemmer AS (
          SELECT stemmer
          FROM fts_main_documents.stats
        ), tokens AS (
          SELECT stem(unnest(fts_main_documents.tokenize($1)), stemmer) AS term
          FROM stemmer
        )
        SELECT dict.term, df
        FROM fts_main_documents.dict AS dict, tokens
        WHERE tokens.term = dict.term
    """
    return con.execute(sql, [query]).fetchall()

def duckdb_search_bm25(con, query, limit, b, k):
    sql = """
        SELECT did, score
        FROM (
            SELECT did, fts_main_documents.match_bm25(did, $1, b=$2, k=$3) AS score
            FROM documents) sq
        WHERE score IS NOT NULL
        ORDER BY score DESC
        LIMIT $4
    """
    return con.execute(sql, [query, b, k, limit]).fetchall()

class Query:
    def __init__(self, query_id, text):
        self.query_id = query_id
        self.text = text


def get_queries_from_file(query_file):
    with open(query_file, "r") as file:
        for line in file:
            (query_id, text) = line.split('\t')
            yield Query(query_id, text)


def get_queries(query_tag):
    if query_tag == "custom":
        from ze_eval import ir_dataset_test
        return ir_dataset_test().queries_iter()
    try:
        return ir_datasets.load(query_tag).queries_iter()
    except KeyError:
        pass
    return get_queries_from_file(query_tag)


def search_run(db_name, query_tag, matcher='lm', run_tag=None,
               b=0.75, k=1.2, limit=1000, fileout=None,
               startq=None, endq=None, verbose=False):
    con = duckdb.connect(db_name, read_only=True)
    if fileout:
        file = open(fileout, "w")
    else:
        file = sys.stdout
    if not run_tag:
        run_tag = matcher
    queries = get_queries(query_tag)
    for query in queries:
        qid = query.query_id
        if (startq and int(qid) < startq) or (endq and int(qid) > endq):
            continue
        if hasattr(query, 'title'):
            q_string = query.title
        else:
            q_string = query.text
        if verbose:
           print(q_string, end='', file=sys.stderr)
           print(duckdb_print_query(con, q_string), file=sys.stderr)
        if matcher == 'lm':
            hits = duckdb_search_lm(con, q_string, limit)
        elif matcher == 'bm25':
            hits = duckdb_search_bm25(con, q_string, limit, b, k)
        else:
            raise ValueError(f"Unknown match function: {matcher}")
        for rank, (docno, score, postings_cost) in enumerate(hits):
            file.write(f'{qid} Q0 {docno} {rank} {score} {run_tag} {postings_cost}\n')
    con.close()
    file.close()


if __name__ == "__main__":
    search_run('cran.db', 'cranfield.tsv')
