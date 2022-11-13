import uuid
from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class Genre:
    id: uuid.uuid4
    name: str
    description: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def get_psql_headers(cls):
        return "id, name, description, created, modified"

    @classmethod
    def get_psql_table_name(cls):
        return "content.genre"

    def values(self):
        return f"{self.id}, {self.name}, {self.description}, {self.created_at}, {self.updated_at}"


@dataclass
class FilmWork:
    id: uuid.uuid4
    title: str
    description: str
    creation_date: date
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def get_psql_headers(cls):
        return "id, title, description, creation_date, rating, type, created, modified"

    @classmethod
    def get_psql_table_name(cls):
        return "content.film_work"

    def values(self):
        return f"{self.id}, {self.title}, {self.description},{self.creation_date}, {self.rating}, {self.type}, {self.created_at}, {self.updated_at}"


@dataclass
class GenreFilmWork:
    id: uuid.uuid4
    genre_id: uuid.uuid4
    film_work_id: uuid.uuid4
    created_at: datetime

    @classmethod
    def get_psql_headers(cls):
        return "id, genre_id, film_work_id, created"

    @classmethod
    def get_psql_table_name(cls):
        return "content.genre_film_work"

    def values(self):
        return f"{self.id}, {self.genre_id}, {self.created_at},"


@dataclass
class Person:
    id: uuid.uuid4
    full_name: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def get_psql_headers(cls):
        return "id, full_name, created, modified"

    @classmethod
    def get_psql_table_name(cls):
        return "content.person"


@dataclass
class PersonFilmWork:
    id: uuid.uuid4
    person_id: uuid.uuid4
    film_work_id: uuid.uuid4
    role: str
    created_at: datetime

    @classmethod
    def get_psql_headers(cls):
        return "id, person_id, film_work_id, role, created"

    @classmethod
    def get_psql_table_name(cls):
        return "content.person_film_work"
