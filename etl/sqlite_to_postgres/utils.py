import contextlib
import sqlite3

import psycopg2


@contextlib.contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@contextlib.contextmanager
def psql_conn_context(
    host: str, port: str, dbname: str, user: str, password: str
):
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    yield conn
    conn.close()
