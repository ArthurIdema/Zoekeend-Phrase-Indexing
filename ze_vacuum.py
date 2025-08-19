import duckdb
import pathlib


def copy_file_force(name_in, name_out):
    path1 = pathlib.Path(name_in)
    if not(path1.is_file()):
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    path2.write_bytes(path1.read_bytes())


def rm_file(name):
    path = pathlib.Path(name)
    path.unlink()


def cluster_index(con):
    con.sql("""
        USE fts_main_documents;
        CREATE TABLE terms_new AS SELECT * FROM terms ORDER BY termid, docid;
        DROP TABLE terms;
        ALTER TABLE terms_new RENAME TO terms;
        CREATE TABLE dict_new AS SELECT * FROM dict ORDER BY term;
        DROP TABLE dict;
        ALTER TABLE dict_new RENAME TO dict;
        CREATE TABLE docs_new AS SELECT * FROM docs ORDER BY docid;
        DROP TABLE docs;
        ALTER TABLE docs_new RENAME TO docs;
    """)

 
def reclaim_disk_space(name, cluster=True):
    # Unfortunately, DuckDB does not reclaim disk space automatically
    # therefore, we do a copy
    tmpname = name + '.tmp'
    copy_file_force(name, tmpname)
    con = duckdb.connect(tmpname)
    if cluster:
        cluster_index(con)
    rm_file(name)
    con.sql(f"""
        ATTACH '{tmpname}' AS tmpdb;
        ATTACH '{name}' AS db;
        COPY FROM DATABASE tmpdb TO db;
    """)
    con.close()
    rm_file(tmpname)

