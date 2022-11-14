import contextlib
import datetime
from dataclasses import dataclass
from typing import List
from uuid import uuid4

import psycopg2
from psycopg2.extensions import cursor
from pydantic import BaseModel
from utils import logger


@contextlib.contextmanager
def psql_conn_context(
    host: str, port: str, dbname: str, user: str, password: str
):
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    yield conn
    conn.close()


class Extractor:
    """Get data from source."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: str = "5433",
        dbname="movies_database",
        user="app",
        password="123qwe",
    ):
        self.conn_details = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    def extract_modified_persons(
        self, cursor: cursor, before: datetime.datetime
    ):
        persons_ids = []
        sql = f"""
            SELECT id, modified
            FROM content.person
            WHERE modified > '{before}'
            ORDER BY modified
            LIMIT 100; 
        """
        cursor.execute(sql)
        data = cursor.fetchall()
        persons_ids = [row[0] for row in data]
        logger.debug("Modified Persons extracted!")
        return persons_ids

    def extract_modified_filmworks(
        self, cursor: cursor, persons_ids: List[uuid4]
    ):
        sql = f"""
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.person_id IN ({','.join([f"'{id}'" for id in persons_ids])})
            ORDER BY fw.modified
            LIMIT 100;
        """
        cursor.execute(sql)
        data = cursor.fetchall()
        filmworks_ids = [row[0] for row in data]
        logger.info("Modified filmworks, based on Persons, extracted!")
        return filmworks_ids

    def get_full_filmwork_data_for_es(
        self, cursor: cursor, filmworks_ids: List[uuid4]
    ):
        """Извлекает нужные поля для отправки в Эластик.

        Сильно связано с Transform классом по порядку извлекаемых полей.
        """
        sql = f"""
            SELECT
                fw.id, 
                fw.title, 
                fw.description, 
                fw.rating, 
                fw.type, 
                fw.created, 
                fw.modified, 
                pfw.role as person_role, 
                p.id as person_id, 
                p.full_name as person_name,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN ({','.join([f"'{id}'" for id in filmworks_ids])});
        """
        cursor.execute(sql)
        meta = cursor.description
        data = cursor.fetchall()
        data = [dict(zip([col.name for col in meta], row)) for row in data]
        logger.info("Full data extracted!")
        return data

    def extract_filmworks(self):
        pass

    def extract(self, before: datetime.datetime):
        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()
            persons_ids = self.extract_modified_persons(cursor, before)
            filmworks = self.extract_modified_filmworks(cursor, persons_ids)
            fullfilled_fillmworks = self.get_full_filmwork_data_for_es(
                cursor, filmworks
            )
        return fullfilled_fillmworks


if __name__ == "__main__":
    now = datetime.datetime.now()
    before = datetime.datetime(2021, 6, 15, 12, 0)
    ex = Extractor()
    ex.extract(before)
