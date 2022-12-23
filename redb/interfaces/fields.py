from dataclasses import dataclass
from enum import Enum

import pymongo
from pydantic import BaseModel
from pydantic.fields import ModelField
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
class Indice:
    field: ModelField
    name: str = None
    unique: bool = None
    direction: Direction = None


@dataclass
class CompoundIndice:
    fields: list[ModelField]
    name: str = None
    unique: bool = None
    direction: Direction = None


class Field(PydanticFieldInfo):
    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
