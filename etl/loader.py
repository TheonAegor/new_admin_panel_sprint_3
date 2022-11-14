import abc
import json
import typing as tp

import requests
from dto import ConnectionDetails, FilmWork, EnrichedFilmWork
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from utils import logger


class IDataAccessor(abc.ABC):
    def push(self, data):
        raise NotImplemented

    def push_bulk(self, data):
        raise NotImplemented


class ElasticAccessor(IDataAccessor):
    def __init__(
        self,
        base_url: str = "http://127.0.0.1",
        port: str = "9200",
        index: str = "movies",
    ):
        self.base_url = base_url
        self.port = port
        self.index = index
        self.elastic = Elasticsearch(hosts=[f"{base_url}:{port}"])
        self.bulk_endpoint = "/_bulk"
        self.single_endpoint = f"/{self.index}/_doc/"

    def gen_data(self, data: tp.List[EnrichedFilmWork]):
        logger.info('Start generating data')
        ret = []
        for filmwork in data:
            print(type(filmwork))
            print(filmwork)
            ret.append(
                {
                    "_index": self.index,
                    "_id": filmwork.id,
                    "actors": [
                        *filmwork.get_actors()
                    ],
                    "actors_names": filmwork.get_actors_names(),
                    "description": filmwork.description,
                    "director": filmwork.director,
                    "genre": filmwork.genre,
                    "id": filmwork.id,
                    "imdb_rating": filmwork.imdb_rating,
                    "title": filmwork.title,
                    "writers": [
                        *filmwork.get_writers()
                    ],
                    "writers_names": filmwork.get_writers_names(),
                }
            )
        return ret

    def push(self, data):
        logger.info("Creating bulk request...")
        # single_data = self.prepare_data_for_single(data)
        # json_single_data = json.dumps(single_data)
        # logger.debug(json_single_data)
        bulk(self.elastic, self.gen_data(data))
        # response = requests.post(
        #     url=f"{self.base_url}:{self.port}{self.single_endpoint}",
        #     json=json_single_data,
        #     headers={"Content-Type": "application/x-ndjson"},
        # )
        # if response.status_code >= 400:
        #     logger.error(response.content)
        logger.info("Bulk request is done!")

    def prepare_data_for_single(self, data: tp.List[FilmWork]):
        ret = ""
        even_line_template = (
            '{{"index": {{"_index": "{index}", "_id": "{film_id}"}}}}\n'
        )
        odd_line_template = """{{ \
            "actors": [{{"id":"{actor_id}","name":"{actor_name}"}}], \
            "actors_names":"{actor_name}", \
            "description":"{description}", \
            "director":"{director}", \
            "genre":"{genre}", \
            "id":"{id}", \
            "imdb_rating":"{imdb_rating}", \
            "title":"{title}", \
            "writers":[{{"id":"{writer_id}","name":"{writer_name}"}}], \
            "writers_names":"{writer_name}" \
            }}\n"""
        for filmwork in data:
            logger.debug(filmwork.title)
            # logger.debug(odd_line_formatted)
            even_line_formatted = even_line_template.format(
                index=self.index, film_id=filmwork.id
            )
            odd_line_formatted = odd_line_template.format(
                actor_id=filmwork.actor_id,
                actor_name=filmwork.actor_name,
                description=filmwork.description,
                genre=filmwork.genre_name,
                id=filmwork.id,
                imdb_rating=filmwork.rating,
                director=filmwork.actor_name,
                title=filmwork.title,
                writer_id=filmwork.actor_id,
                writer_name=filmwork.actor_name,
            )
            ret += even_line_formatted
            ret += odd_line_formatted
        return ret

    def push_batch(self, data):
        bulk_data = self.prepare_data_for_bulk(data)
        requests.post(
            url=f"{self.base_url}{self.port}{self.bulk_endpoint}",
            data=bulk_data,
        )


class Loader:
    def __init__(
        self,
        data: tp.List[tp.Any],
        conn_details: ConnectionDetails = {},
        data_accessor: IDataAccessor = ElasticAccessor,
        **extra_accessor_kwargs,
    ):
        self.data_accessor = data_accessor(
            **conn_details, **extra_accessor_kwargs
        )
        self.data = data

    def load(self):
        self.data_accessor.push(self.data)
