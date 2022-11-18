import contextlib
import datetime
from typing import Generator, List, Literal, Optional
from uuid import uuid4

import psycopg2
from dto import FilmWork
from psycopg2.extensions import cursor
from sql_utils import full_filmwork_data_sql_template
from state import state
from utils import gen_backoff, logger


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

    # @backoff()
    def make_query(self, cursor: cursor, sql: str):
        return cursor.execute(sql)

    def extract_modified_entities(
        self,
        cursor: cursor,
        entity: str = "person",
        limit: Optional[str] = None,
        time_of_run: Optional[datetime.datetime] = None,
    ) -> Generator[List[str], None, None]:
        while True:
            excepted_ids = state.get_state(f"{entity}_excepted_ids")
            if not time_of_run:
                time_of_run = state.get_state("time_of_run")

            entities_ids = []
            lim = f"LIMIT {limit}" if limit else ""
            where = (
                f"""and id NOT IN ({','.join([f"'{id}'" for id in excepted_ids])})"""
                if excepted_ids
                else ""
            )
            sql = f"""
                SELECT id, modified
                FROM content.{entity}
                WHERE modified > '{time_of_run}' {where}
                ORDER BY modified
                {lim};
            """
            self.make_query(cursor, sql)
            entity_data = cursor.fetchall()
            if not entity_data:
                logger.debug("Stop iteration in extract_modified_entities")
                return
            entities_ids = [row[0] for row in entity_data]

            ei = state.get_state(f"{entity}_excepted_ids")
            state.set_state(
                f"{entity}_excepted_ids",
                [*entities_ids] if not ei else [*ei, *entities_ids],
            )
            if entities_ids:
                logger.info(
                    f"{len(entities_ids)} rows has been extracted, {entity}!"
                )
            yield entities_ids

    def extract_modified_filmworks(
        self,
        cursor: cursor,
        entity_ids: Generator[List[uuid4], None, None],
        entity: str = "person",
        limit: Optional[str] = None,
    ) -> Generator[List[str], None, None]:
        for batch in entity_ids:
            if entity == "film_work":
                yield batch
            lim = f"LIMIT {limit}" if limit else ""

            sql = f"""
                SELECT fw.id, fw.modified
                FROM content.film_work fw
                LEFT JOIN content.{entity}_film_work pfw ON pfw.film_work_id = fw.id
                WHERE pfw.{entity}_id IN ({','.join([f"'{id}'" for id in batch])})
                ORDER BY fw.modified
                {lim};
            """
            self.make_query(cursor, sql)
            filmworks = cursor.fetchall()
            if not filmworks:
                return
            filmworks_ids = [row[0] for row in filmworks]

            logger.info(
                f"{len(filmworks_ids)} has been extracted, \
                    film_work, based on {entity}!"
            )
            yield filmworks_ids

    def get_full_filmwork_data_for_es(
        self,
        cursor: cursor,
        filmworks_ids: Generator[List[uuid4], None, None],
        limit: Optional[str] = None,
        where: bool = True,
    ) -> Generator[List[FilmWork], None, None]:
        """Извлекает нужные поля для отправки в Эластик."""
        for batch in filmworks_ids:
            lim = f"LIMIT {limit}" if limit else ""
            where = (
                f"""WHERE fw.id IN ({','.join([f"'{id}'" for id in batch])})"""
                if where
                else ""
            )
            sql = full_filmwork_data_sql_template % {
                "lim": lim,
                "where": where,
            }

            self.make_query(cursor, sql)
            meta = cursor.description
            fullfilled_fws = cursor.fetchall()
            if not fullfilled_fws:
                raise StopIteration()
            fullfilled_fws = [
                dict(zip([col.name for col in meta], row))
                for row in fullfilled_fws
            ]
            logger.info(
                f"{len(fullfilled_fws)} rows has been extracted, full_film_work!"
            )
            yield fullfilled_fws

    def extract(self, time_of_run: datetime.datetime) -> List[FilmWork]:
        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()
            persons_ids = self.extract_modified_entities(cursor, time_of_run)
            filmworks = self.extract_modified_filmworks(cursor, persons_ids)
            fullfilled_fillmworks = self.get_full_filmwork_data_for_es(
                cursor, filmworks
            )
        return fullfilled_fillmworks


class ExtractEntity(Extractor):
    @gen_backoff()
    def extract(
        self,
        entity: Literal["genre", "person", "film_work"],
    ) -> Generator[List[FilmWork], None, None]:

        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()

            entities_ids_gen = self.extract_modified_entities(
                cursor=cursor,
                entity=entity,
                limit="100",
            )
            filmworks_gen = self.extract_modified_filmworks(
                cursor=cursor, entity_ids=entities_ids_gen, entity=entity
            )
            fullfilled_filmworks_gen = self.get_full_filmwork_data_for_es(
                cursor=cursor, filmworks_ids=filmworks_gen, where=True
            )
            for batch in fullfilled_filmworks_gen:
                yield batch

    @gen_backoff()
    def extract_only_fw(self) -> Generator[List[FilmWork], None, None]:
        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()
            entities_ids_gen = self.extract_modified_entities(
                cursor=cursor,
                entity="film_work",
                limit=None,
                time_of_run=datetime.datetime.min,
            )
            filmworks_gen = self.extract_modified_filmworks(
                cursor=cursor, entity_ids=entities_ids_gen, entity="film_work"
            )
            fullfilled_filmworks_gen = self.get_full_filmwork_data_for_es(
                cursor=cursor, filmworks_ids=filmworks_gen, where=False
            )
            for batch in fullfilled_filmworks_gen:
                yield batch


if __name__ == "__main__":
    now = datetime.datetime.now()
    before = datetime.datetime(2021, 6, 15, 12, 0)
    ex = Extractor()
    ex.extract(before)
