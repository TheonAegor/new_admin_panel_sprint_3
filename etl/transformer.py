from dto import EnrichedFilmWork, Person
from utils import logger


class Transformer:
    def __init__(self, raw_objects: list[any]):
        self.raw_objects = raw_objects

    def transform_to_objects(self):
        objects = {}
        if self.raw_objects:
            for fw in self.raw_objects:
                if fw["id"] not in objects:
                    objects[fw["id"]] = self.build_enrichedfw(fw)
                else:
                    self.add_info_to_fw(objects[fw["id"]], fw)
        self.objects = objects
        return list(self.objects.values())

    def add_info_to_fw(self, fw_object: EnrichedFilmWork, fw):
        role = fw["person_role"]
        person_name = fw["person_name"]
        person = Person(fw["person_id"], person_name)
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

    def build_enrichedfw(self, fw):
        director = "director" if fw["person_role"] == "director" else ""
        actors = (
            [Person(fw["person_id"], fw["person_name"])]
            if fw["person_role"] == "actor"
            else []
        )
        actors_names = (
            [fw["person_name"]] if fw["person_role"] == "actor" else []
        )
        writers = (
            [Person(fw["person_id"], fw["person_name"])]
            if fw["person_role"] == "writer"
            else []
        )
        writers_names = (
            [fw["person_name"]] if fw["person_role"] == "writer" else []
        )

        ret = EnrichedFilmWork(
            id=fw["id"],
            title=fw["title"],
            description=fw["description"],
            imdb_rating=fw["rating"],
            type=fw["type"],
            director=director,
            created=fw["created"],
            modified=fw["modified"],
            actors=actors,
            actors_names=actors_names,
            writers=writers,
            writers_names=writers_names,
            genre=fw["genre_name"],
        )
        return ret

    def transform_for_es(self):
        logger.debug("Start transforming objects...")
        if not len(self.raw_objects):
            logger.info("Objects to transform are empty!")
            return self.raw_objects
        objects = self.transform_to_objects()
        logger.info("Objects transformed!")
        return objects
