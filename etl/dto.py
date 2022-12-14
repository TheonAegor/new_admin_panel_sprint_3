import datetime
from dataclasses import asdict, dataclass, fields

from pydantic import BaseModel


@dataclass
class FilmWork:
    id: str
    title: str
    description: str
    rating: float
    type: str
    created: datetime.datetime
    modified: datetime.datetime
    personrole: str
    person_id: str
    person_name: str
    genre_name: str

    @classmethod
    def get_fields(cls):
        return [field.name for field in fields(cls)]


@dataclass
class Person:
    id: str
    name: str


@dataclass
class EnrichedFilmWork:
    id: str
    title: str
    description: str
    imdb_rating: float
    type: str
    director: str
    created: datetime.datetime
    modified: datetime.datetime
    actors: list[Person]
    actors_names: list[str]
    writers: list[Person]
    writers_names: list[str]
    genre: str

    def get_actors(self):
        return [asdict(actor) for actor in self.actors]

    def get_writers(self):
        return [asdict(writer) for writer in self.writers]

    def get_actors_names(self):
        return ",".join(self.actors_names)

    def get_writers_names(self):
        return ",".join(self.writers_names)


class ConnectionDetails(BaseModel):
    base_url: str
    port: str
