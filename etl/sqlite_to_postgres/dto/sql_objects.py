from dataclasses import dataclass
from typing import List, Mapping, Tuple


@dataclass
class SQLiteRow:
    result: List[Mapping]


@dataclass
class PsqlRes:
    result: List[Tuple]
