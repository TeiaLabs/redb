from dataclasses import dataclass
from enum import Enum
from typing import Any, ForwardRef, TypeVar

import pymongo
from pydantic import BaseModel
from pydantic.fields import ModelField
from pydantic.main import FieldInfo as PydanticFieldInfo

T = TypeVar("T")


class Direction(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING


class Column(BaseModel):
    name: str


class IncludeColumn(Column):
    include: bool


class SortColumn(Column):
    direction: Direction


@dataclass
class Indice:
    field: ModelField
    name: str | None = None
    unique: bool = False
    direction: Direction | None = None


@dataclass
class CompoundIndice:
    fields: list[ModelField]
    name: str | None = None
    unique: bool = False
    direction: Direction | None = None


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
    def __init__(self, model_field: ModelField, base_class: BaseModel):
        self.model_field = model_field
        self.base_class = base_class
        self.attr_names = [model_field.alias]

    def resolve(self, obj: T) -> T | None:
        for attr_name in self.attr_names:
            if not obj:
                return None

            obj = getattr(obj, attr_name)

        return obj

    def __getattribute__(self, name: str) -> Any:
        if name in {"model_field", "base_class", "attr_names", "resolve"}:
            return super().__getattribute__(name)

        try:
            annotation = __get_unwrapped_annotation(self.model_field.annotation)
            base_class = __get_type_from_annotation(annotation, self.base_class)

            if not hasattr(base_class, "__fields__"):
                raise AttributeError

            fields = base_class.__fields__
            if name not in fields:
                raise AttributeError

            self.model_field = fields[name]
            self.base_class = base_class
            self.attr_names.append(name)
            return self
        except AttributeError:
            return self.model_field.__getattribute__(name)

    def __getitem__(self, _):
        return self


def __get_unwrapped_annotation(annotation: T) -> T:
    annotation = __unwrap_optional(annotation)
    annotation = __unwrap_iterable(annotation)
    return annotation


def __unwrap_optional(annotation: T) -> T:
    if hasattr(annotation, "_name") and annotation._name == "Optional":
        return annotation.__args__[0]
    return annotation


def __unwrap_iterable(annotation: T) -> T:
    if hasattr(annotation, "__args__"):
        if annotation.__origin__ not in {list, set}:
            raise ValueError("Are you trying to outsmart me!? HA HA")
        return annotation.__args__[0]
    return annotation


def __get_type_from_annotation(annotation: T, base_class: BaseModel) -> BaseModel:
    if isinstance(annotation, ForwardRef):
        if not annotation.__forward_evaluated__:
            base_class.update_forward_refs()
        return annotation.__forward_value__
    return annotation
