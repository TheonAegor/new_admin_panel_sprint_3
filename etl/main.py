from datetime import datetime

from extractor import Extractor
from loader import Loader
from transformer import Transformer
from utils import logger


def main():
    before = datetime(2021, 6, 15, 12, 0)
    ex = Extractor()
    data = ex.extract(before)
    trans = Transformer(data)
    transformed_data = trans.transform_for_es()
    loader = Loader(transformed_data)
    loader.load()


if __name__ == "__main__":
    main()
