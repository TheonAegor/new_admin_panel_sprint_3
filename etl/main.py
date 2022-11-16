from time import sleep

from extractor import ExtractEntity
from loader import Loader
from state import get_all_keys_and_values, state
from transformer import Transformer
from utils import logger


def main():
    """
    pseudocode:
        time_of_last_update = now

        is_first_run = get from state

        if first_run:
            extract batch of fw
            transform it
            load it
            write to state

        launch fw, genre, person check for updates
            if updates:
                extract batch of records (generator)
                    write in state ids that have been processed
                transform batch of records (generator)
                    write in state ids that have been processed
                load batch of records


    (
        определяем время последнего изменения,
        если есть изменения в person, genre, filmwork -
            прогоняем через трубу,
            добавляем ай-дишники в состояние,
            сохраняем время самой поздней модификации
        Устанавливаем время самой поздней модификации как time_of_last_update
    )
    """
    # # time_of_run = datetime(2021, 6, 15, 12, 0)

    is_first_run = state.get_state("is_first_run")

    if is_first_run is None:
        state.set_state("is_first_run", True)
        is_first_run = True

    while True:
        ex = ExtractEntity()
        time_of_run = state.get_state("time_of_run")
        if is_first_run:
            fws = ex.extract_only_fw()
            trans = Transformer(fws)
            transformed_data = trans.transform_for_es()
            loader = Loader(transformed_data)
            loader.load()
            state.set_state("is_first_run", False)
        for entity in ["genre", "person", "film_work"]:
            logger.info(f"Looking for changes in {entity}")
            for entity_data in ex.extract(
                time_of_run=time_of_run, entity=entity
            ):
                trans = Transformer(entity_data)
                transformed_data = trans.transform_for_es()
                loader = Loader(transformed_data)
                loader.load()
            state.set_state(f"{entity}_excepted_ids", None)
        logger.info(get_all_keys_and_values(state))
        sleep(5)


if __name__ == "__main__":
    main()
