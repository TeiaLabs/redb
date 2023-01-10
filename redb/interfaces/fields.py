from dataclasses import dataclass
from enum import Enum
from typing import Any, ForwardRef

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
class Index:
    field: ModelField
    name: str = None
    unique: bool = False
    direction: Direction = None


@dataclass
class CompoundIndex:
    fields: list[ModelField]
    name: str = None
    unique: bool = False
    direction: Direction = None


class Field(PydanticFieldInfo):
    def __init__(
        self,
        hashable: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.hashable = hashable


class ClassField:
    def __init__(self, model_field: ModelField, base_class: BaseModel) -> None:
        self.model_field = model_field
        self.base_class = base_class

    def __getattribute__(self, name: str) -> Any:
        if name in {"model_field", "base_class"}:
            return object.__getattribute__(self, name)

        model_field = self.model_field
        try:
            base_class = None
            annotation = model_field.annotation
            if isinstance(annotation, ForwardRef):
                if not annotation.__forward_evaluated__:
                    self.base_class.update_forward_refs()

                forwarded: BaseModel = annotation.__forward_value__
                fields = forwarded.__fields__
                base_class = forwarded
            else:
                if not hasattr(annotation, "__fields__"):
                    raise AttributeError

                fields = annotation.__fields__
                base_class = annotation

            if name not in fields:
                raise AttributeError

            return ClassField(model_field=fields[name], base_class=base_class)
        except AttributeError:
            return model_field.__getattribute__(name)
