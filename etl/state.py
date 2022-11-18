import abc
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

from redis import Redis


def get_default(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        state_data = self.redis_adapter.get("data")
        if state_data is None:
            self.redis_adapter.set("data", "{}")
            state_data = {}
        else:
            state_data = json.loads(state_data)
        self.state_data = state_data

    def save_state(self, state: dict) -> None:
        """Я и хотел хранить в отдельном ключе, но меня сбили с толку тесты
            для задания со звездочкой, где FakeRedis проверял все в ключе
            data."""
        self.state_data = json.loads(
            (self.redis_adapter.get("data")).decode("utf-8")
        )
        self.state_data |= state
        self.redis_adapter.set(
            "data", json.dumps(self.state_data, default=get_default)
        )

    def retrieve_state(self, key: str) -> dict:
        state_data = json.loads(self.redis_adapter.get("data"))
        ret = state_data.get(key, None)
        return ret


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, key_value: any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: key_value})

    def get_state(self, key: str) -> any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state(key)
        return state


def get_all_keys_and_values(state: State):
    storage_adapter = state.storage.redis_adapter
    ret = {}
    for key in storage_adapter.scan_iter():
        ret[key] = storage_adapter.get(key)
    return ret


def empty_state(state: State, keys: list[str]):
    for key in keys:
        state.set_state(key, None)


zone = ZoneInfo("Etc/GMT-3")
storage = RedisStorage(Redis())

state = State(storage=storage)
if state.get_state("time_of_run") is None:
    state.set_state("time_of_run", datetime.now(zone))

empty_state(
    state,
    ["film_work_excepted_ids", "person_excepted_ids", "genre_excepted_ids"],
)
