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
    def __init__(
        self,
        model_field: ModelField,
        base_class: BaseModel,
    ) -> None:
        self.model_field = model_field
        self.base_class = base_class
        self.attr_names = [model_field.alias]

    def resolve(self) -> str:
        return ".".join([attr_name for attr_name in self.attr_names])

    def __getattribute__(self, name: str) -> Any:
        if name in {"model_field", "base_class", "attr_names", "resolve"}:
            return super().__getattribute__(name)

        model_field = self.model_field
        try:
            base_class = None
            annotation = model_field.annotation
            if hasattr(annotation, "_name") and annotation._name == "Optional":
                annotation = annotation.__args__[0]

            if hasattr(annotation, "__args__"):
                if annotation.__origin__ not in {list, set}:
                    raise ValueError("Are you trying to outsmart me!? HA HA")

                annotation = annotation.__args__[0]

            if isinstance(annotation, ForwardRef):
                if not annotation.__forward_evaluated__:
                    self.base_class.update_forward_refs()

                base_class = annotation.__forward_value__
            else:
                base_class = annotation

            if not hasattr(annotation, "__fields__"):
                raise AttributeError

            fields = base_class.__fields__
            if name not in fields:
                raise AttributeError

            self.model_field = fields[name]
            self.base_class = base_class
            self.attr_names.append(name)
            return self
        except AttributeError:
            return model_field.__getattribute__(name)

    def __getitem__(self, _):
        return self
