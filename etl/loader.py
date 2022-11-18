import abc

import requests
from dto import ConnectionDetails, EnrichedFilmWork
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from utils import backoff, logger


class IDataAccessor(abc.ABC):
    @abc.abstractmethod
    def push(self, index_data):
        pass

    @abc.abstractmethod
    def push_bulk(self, index_data):
        pass


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
        self._post_init()

    @backoff()
    def if_index_not_exist(self):
        request_body = {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_",
                        },
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english",
                        },
                        "english_possessive_stemmer": {
                            "type": "stemmer",
                            "language": "possessive_english",
                        },
                        "russian_stop": {
                            "type": "stop",
                            "stopwords": "_russian_",
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian",
                        },
                    },
                    "analyzer": {
                        "ru_en": {
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "english_stop",
                                "english_stemmer",
                                "english_possessive_stemmer",
                                "russian_stop",
                                "russian_stemmer",
                            ],
                        }
                    },
                },
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "imdb_rating": {"type": "float"},
                    "genre": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "fields": {"raw": {"type": "keyword"}},
                    },
                    "description": {"type": "text", "analyzer": "ru_en"},
                    "director": {"type": "text", "analyzer": "ru_en"},
                    "actors_names": {"type": "text", "analyzer": "ru_en"},
                    "writers_names": {"type": "text", "analyzer": "ru_en"},
                    "actors": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "text", "analyzer": "ru_en"},
                        },
                    },
                    "writers": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "name": {"type": "text", "analyzer": "ru_en"},
                        },
                    },
                },
            },
        }
        if not self.elastic.indices.exists(index=self.index):
            logger.info("Index didn't exist. Creating 'movies'...")
            self.elastic.indices.create(index=self.index, body=request_body)
            logger.info(f"Index {self.index} succesfully created!")

    def gen_data(self, filmworks: list[EnrichedFilmWork]):
        logger.info("Start generating data")
        ret = []
        for filmwork in filmworks:
            ret.append(
                {
                    "_index": self.index,
                    "_id": filmwork.id,
                    "actors": [*filmwork.get_actors()],
                    "actors_names": filmwork.get_actors_names(),
                    "description": filmwork.description,
                    "director": filmwork.director,
                    "genre": filmwork.genre,
                    "id": filmwork.id,
                    "imdb_rating": filmwork.imdb_rating,
                    "title": filmwork.title,
                    "writers": [*filmwork.get_writers()],
                    "writers_names": filmwork.get_writers_names(),
                }
            )
        return ret

    @backoff()
    def push(self, index_data):
        logger.info("Creating bulk request...")
        bulk(self.elastic, self.gen_data(index_data))
        logger.info("Bulk request is done!")

    def push_batch(self, index_data):
        bulk_data = self.prepare_data_for_bulk(index_data)
        requests.post(
            url=f"{self.base_url}{self.port}{self.bulk_endpoint}",
            data=bulk_data,
        )

    def _post_init(self):
        self.if_index_not_exist()


class Loader:
    def __init__(
        self,
        index_data: list[any],
        conn_details: ConnectionDetails = {},
        data_accessor: IDataAccessor = ElasticAccessor,
        **extra_accessor_kwargs,
    ):
        self.data_accessor = data_accessor(
            **conn_details, **extra_accessor_kwargs
        )
        self.index_data = index_data

    def load(self):
        logger.info("Start loading to Elastic objects...")
        if not len(self.index_data):
            logger.info("Objects to load are empty!")
            return
        self.data_accessor.push(self.index_data)
        logger.info("Objects loaded!")
