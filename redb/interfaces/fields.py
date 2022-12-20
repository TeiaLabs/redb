from dataclasses import dataclass
from enum import Enum

import pymongo
from pydantic import BaseModel
from pydantic.main import FieldInfo as PydanticFieldInfo


class Direction(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING


class DBColumn(BaseModel):
    name: str


class IncludeDBColumn(DBColumn):
    include: bool


class SortDBColumn(DBColumn):
    direction: Direction


@dataclass
class Index:
    names: list[str] = None
    unique: bool = None
    directions: list[Direction] = None


@dataclass
class FieldIndice:
    group_name: str = None
    name: str = None
    unique: bool = None
    order: int = None
    direction: Direction = None


class Field(PydanticFieldInfo):
    def __init__(
        self,
        index: Index = None,
        *args,
        **kwargs,
    ) -> None:
        self.index = index
        super().__init__(*args, **kwargs)
