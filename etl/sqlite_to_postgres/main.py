import os
import typing as tp
from dataclasses import asdict, astuple

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from dto.movies_objects import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)
from dto.sql_objects import PsqlRes, SQLiteRow
from utils import conn_context, psql_conn_context

load_dotenv()
objects = [FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork]
tables = os.getenv("TABLES").split(",")
psqlpassword = os.getenv("PSQLPASSWORD")
psqlhostname = os.getenv("PSQLHOSTNAME")
psqldbname = os.getenv("PSQLDBNAME")
psqluser = os.getenv("PSQLUSER")
psqlport = os.getenv("PSQLPORT")


def pick_object(
    table: str,
) -> None | FilmWork | Genre | GenreFilmWork | Person | PersonFilmWork:
    ret = None
    match table:
        case "film_work":
            ret = FilmWork
        case "genre":
            ret = Genre
        case "genre_film_work":
            ret = GenreFilmWork
        case "person":
            ret = Person
        case "person_film_work":
            ret = PersonFilmWork
    return ret


def main() -> None:
    batch_size = int(os.getenv("BATCH_SIZE"))

    with psql_conn_context(
        psqlhostname, psqlport, psqldbname, psqluser, psqlpassword
    ) as psqlconn:
        psqlcur = psqlconn.cursor()
        with conn_context(os.getenv("DB_NAME")) as conn:
            curs = conn.cursor()
            for i in tables:
                if i is None or i == "":
                    continue
                dto = pick_object(i)
                curs.execute(f"SELECT * FROM {i};")
                while data := curs.fetchmany(batch_size):
                    batch_to_insert = []
                    insert_template = ""
                    for r in data:
                        r_dict = {
                            k: v
                            for k, v in dict(r).items()
                            if k in dto.__dataclass_fields__.keys()
                        }
                        batch_to_insert.append(dto(**r_dict))
                    objs = [astuple(o) for o in batch_to_insert]
                    insert_template = ",".join(["%s"] * len(objs))
                    insert_sql = "insert into {0} ({1}) values {2} ON CONFLICT DO NOTHING;".format(
                        dto.get_psql_table_name(),
                        dto.get_psql_headers(),
                        insert_template,
                    )
                    psqlcur.execute(insert_sql, objs)
                    psqlconn.commit()
    print("All tables moved to Postgres!")


def after_test():
    with psql_conn_context(
        psqlhostname, psqlport, psqldbname, psqluser, psqlpassword
    ) as psqlconn:
        psqlcurr = psqlconn.cursor()
        with conn_context(os.getenv("DB_NAME")) as liteconn:
            litecurr = liteconn.cursor()
            for i in tables:
                if i is None or i == "":
                    break
                postgre_sql = f"SELECT COUNT(*) from content.{i};"
                lite_sql = f"SELECT COUNT(*) from {i};"

                psqlcurr.execute(postgre_sql)
                litecurr.execute(lite_sql)

                psql_res = PsqlRes(psqlcurr.fetchall())
                lite_res = SQLiteRow(
                    dict(*[dict(r) for r in litecurr.fetchall()])
                )

                psql_val = psql_res.result[0][0]
                lite_val = list(lite_res.result.values())[0]

                assert psql_val == lite_val
    print("All tests passed!")


if __name__ == "__main__":
    main()
    after_test()
