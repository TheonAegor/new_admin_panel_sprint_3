from dto import EnrichedFilmWork, Person
from utils import logger


class Transformer:
    def __init__(self, raw_objects: list[any]):
        self.raw_objects = raw_objects

    def transform_to_objects(self):
        objects = {}
        if self.raw_objects:
            for filmwork in self.raw_objects:
                if filmwork["id"] not in objects:
                    objects[filmwork["id"]] = self.build_enrichedfw(filmwork)
                else:
                    self.add_info_to_filmwork(objects[filmwork["id"]], filmwork)
        self.objects = objects
        return list(self.objects.values())

    def add_info_to_filmwork(self, fw_object: EnrichedFilmWork, filmwork: dict):
        role = filmwork["person_role"]
        person_name = filmwork["person_name"]
        person = Person(filmwork["person_id"], person_name)
        match role:
            case "actor":
                if person not in fw_object.actors:
                    fw_object.actors.append(person)
                    fw_object.actors_names.append(person_name)
            case "writer":
                if person not in fw_object.writers:
                    fw_object.writers.append(person)
                    fw_object.writers_names.append(person_name)
            case "director":
                fw_object.director = person_name

    def build_enrichedfw(self, filmwork):
        director = "director" if filmwork["person_role"] == "director" else ""
        actors = (
            [Person(filmwork["person_id"], filmwork["person_name"])]
            if filmwork["person_role"] == "actor"
            else []
        )
        actors_names = (
            [filmwork["person_name"]] if filmwork["person_role"] == "actor" else []
        )
        writers = (
            [Person(filmwork["person_id"], filmwork["person_name"])]
            if filmwork["person_role"] == "writer"
            else []
        )
        writers_names = (
            [filmwork["person_name"]] if filmwork["person_role"] == "writer" else []
        )

        ret = EnrichedFilmWork(
            id=filmwork["id"],
            title=filmwork["title"],
            description=filmwork["description"],
            imdb_rating=filmwork["rating"],
            type=filmwork["type"],
            director=director,
            created=filmwork["created"],
            modified=filmwork["modified"],
            actors=actors,
            actors_names=actors_names,
            writers=writers,
            writers_names=writers_names,
            genre=filmwork["genre_name"],
        )
        return ret

    def transform_for_es(self):
        if not len(self.raw_objects):
            return self.raw_objects
        objects = self.transform_to_objects()
        return objects
