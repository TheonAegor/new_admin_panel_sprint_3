import contextlib
import datetime
from typing import List, Literal, Optional
from uuid import uuid4

import psycopg2
from dto import FilmWork
from psycopg2.extensions import cursor
from state import state, zone
from utils import backoff, logger


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

    def extract_modified_entities(
        self,
        cursor: cursor,
        time_of_run: datetime.datetime,
        excepted_ids: List[str],
        entity: str = "person",
        limit: Optional[str] = None,
        where: bool = True,
    ):
        entities_ids = []
        lim = f"LIMIT {limit}" if limit else ""
        where = (
            f"""and id NOT IN ({','.join([f"'{id}'" for id in excepted_ids])})"""
            if where and excepted_ids
            else ""
        )
        sql = f"""
            SELECT id, modified
            FROM content.{entity}
            WHERE modified > '{time_of_run}' {where}
            ORDER BY modified
            {lim};
        """
        cursor.execute(sql)
        entity_data = cursor.fetchall()
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
            state.set_state("time_of_run", datetime.datetime.now(zone))
        return entities_ids

    def extract_modified_filmworks(
        self,
        cursor: cursor,
        entity_ids: List[uuid4],
        entity: str = "person",
        limit: Optional[str] = None,
    ):
        if entity == "film_work":
            return entity_ids
        lim = f"LIMIT {limit}" if limit else ""
        sql = f"""
            SELECT fw.id, fw.modified
            FROM content.film_work fw
            LEFT JOIN content.{entity}_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.{entity}_id IN ({','.join([f"'{id}'" for id in entity_ids])})
            ORDER BY fw.modified
            {lim};
        """
        cursor.execute(sql)
        filmworks = cursor.fetchall()
        filmworks_ids = [row[0] for row in filmworks]

        logger.info(
            f"{len(filmworks_ids)} has been extracted, film_work, based on {entity}!"
        )
        return filmworks_ids

    def get_full_filmwork_data_for_es(
        self,
        cursor: cursor,
        filmworks_ids: List[uuid4],
        limit: Optional[str] = None,
        where: bool = True,
    ) -> List[FilmWork]:
        """Извлекает нужные поля для отправки в Эластик."""
        lim = f"LIMIT {limit}" if limit else ""
        where = (
            f"""WHERE fw.id IN ({','.join([f"'{id}'" for id in filmworks_ids])})"""
            if where
            else ""
        )
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
            {where}
            {lim};
        """
        cursor.execute(sql)
        meta = cursor.description
        fullfilled_fws = cursor.fetchall()
        fullfilled_fws = [dict(zip([col.name for col in meta], row)) for row in fullfilled_fws]
        logger.info(f"{len(fullfilled_fws)} rows has been extracted, full_film_work!")
        return fullfilled_fws

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
    @backoff
    def extract(
        self,
        time_of_run: datetime.datetime,
        entity: Literal["genre", "person", "film_work"],
    ) -> List[FilmWork]:

        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()

            while entities_ids := self.extract_modified_entities(
                cursor=cursor,
                time_of_run=time_of_run,
                entity=entity,
                limit="100",
                where=True,
                excepted_ids=state.get_state(f"{entity}_excepted_ids"),
            ):
                filmworks = self.extract_modified_filmworks(
                    cursor=cursor, entity_ids=entities_ids, entity=entity
                )
                fullfilled_filmworks = self.get_full_filmwork_data_for_es(
                    cursor=cursor, filmworks_ids=filmworks, where=True
                )
                yield fullfilled_filmworks

    @backoff
    def extract_only_fw(self) -> List[FilmWork]:
        with psql_conn_context(**self.conn_details) as connection:
            cursor = connection.cursor()
            fullfilled_filmworks = self.get_full_filmwork_data_for_es(
                cursor=cursor, filmworks_ids=[], where=False
            )

        return fullfilled_filmworks


if __name__ == "__main__":
    now = datetime.datetime.now()
    before = datetime.datetime(2021, 6, 15, 12, 0)
    ex = Extractor()
    ex.extract(before)
