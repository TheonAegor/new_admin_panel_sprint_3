import logging
import logging.config
from datetime import datetime
from functools import wraps
from time import sleep

from psycopg2 import OperationalError

ERROR_LOG_FILENAME = "logs.log"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s:%(name)s:%(process)d:%(lineno)d "
            "%(levelname)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "logfile": {
            "formatter": "default",
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": ERROR_LOG_FILENAME,
            "backupCount": 2,
        }
    },
    "loggers": {
        "simple": {
            "level": "DEBUG",
            "handlers": [
                "logfile",
            ],
        },
    },
    "root": {},
}

# logging.basicConfig(level=logging.DEBUG)
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("simple")


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если
        возникла ошибка. Использует наивный экспоненциальный рост времени повтора
        (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):

        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            ret = None
            while sleep_time <= border_sleep_time:
                sleep(sleep_time)
                try:
                    ret = func(*args, **kwargs)
                except OperationalError:
                    logger.error(
                        f"[{datetime.now().isoformat()}]Can not connect to db!"
                    )
                    sleep_time = sleep_time * factor
                    if sleep_time > border_sleep_time:
                        sleep_time = border_sleep_time
                except Exception as e:
                    logger.error(e)
                    sleep_time = sleep_time * factor
                    if sleep_time > border_sleep_time:
                        sleep_time = border_sleep_time
                else:
                    return ret
            return ret

        return inner

    return func_wrapper


def gen_backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            ret = None
            while sleep_time <= border_sleep_time:
                sleep(sleep_time)
                try:
                    for ret in func(*args, **kwargs):
                        yield ret
                except OperationalError:
                    logger.error(
                        f"[{datetime.now().isoformat()}]Can not connect to db!"
                    )
                    sleep_time = sleep_time * factor
                    if sleep_time > border_sleep_time:
                        sleep_time = border_sleep_time
                except Exception as e:
                    logger.error(e)
                    sleep_time = sleep_time * factor
                    if sleep_time > border_sleep_time:
                        sleep_time = border_sleep_time
                else:
                    return ret
            return ret

        return inner

    return func_wrapper
