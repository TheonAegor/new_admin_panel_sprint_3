from dataclasses import dataclass
from typing import Mapping, Tuple, list


@dataclass
class SQLiteRow:
    result: list[Mapping]


@dataclass
class PsqlRes:
    result: list[Tuple]
