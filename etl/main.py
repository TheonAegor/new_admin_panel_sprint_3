import datetime
from time import sleep

from extractor import ExtractEntity
from loader import Loader
from state import get_all_keys_and_values, state, zone
from transformer import Transformer
from utils import logger


def main():
    is_first_run = state.get_state("is_first_run")

    if is_first_run is None:
        state.set_state("is_first_run", True)
        is_first_run = True

    state.set_state("film_work_excepted_ids", None)
    while True:
        ex = ExtractEntity()
        if is_first_run:
            for batch in ex.extract_only_fw():
                trans = Transformer(batch)
                transformed_data = trans.transform_for_es()
                loader = Loader(transformed_data)
                loader.load()
            state.set_state("is_first_run", False)
            state.set_state("film_work_excepted_ids", None)
        for entity in ["genre", "person", "film_work"]:
            logger.info(f"Looking for changes in {entity}")
            for entity_data in ex.extract(entity=entity):
                trans = Transformer(entity_data)
                transformed_data = trans.transform_for_es()
                loader = Loader(transformed_data)
                loader.load()
            if state.get_state(f"{entity}_excepted_ids"):
                state.set_state("time_of_run", datetime.datetime.now(zone))
            state.set_state(f"{entity}_excepted_ids", None)
        logger.info(get_all_keys_and_values(state))
        sleep(1)


if __name__ == "__main__":
    main()
