from dataclasses import dataclass
from enum import Enum
from typing import Any, ForwardRef, TypeVar, Union

import pymongo
from pydantic import BaseModel
from pydantic.fields import FieldInfo, ModelField
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)

T = TypeVar("T")

PyMongoOperations = TypeVar(
    "PyMongoOperations",
    bound=Union[
        InsertOne,
        DeleteOne,
        DeleteMany,
        ReplaceOne,
        UpdateOne,
        UpdateMany,
    ],
)


class Direction(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING
    GEO2D = pymongo.GEO2D
    GEOSPHERE = pymongo.GEOSPHERE
    HASHED = pymongo.HASHED
    TEXT = pymongo.TEXT


class Column(BaseModel):
    name: str


class IncludeColumn(Column):
    include: bool


class SortColumn(Column):
    direction: Direction


@dataclass
class Index:
    field: "ClassField"
    name: str | None = None
    unique: bool = False
    direction: Direction = Direction.ASCENDING
    extras: dict | None = None


@dataclass
class CompoundIndex:
    fields: list["ClassField"]
    name: str | None = None
    unique: bool = False
    direction: Direction = Direction.ASCENDING
    extras: dict | None = None


class Field(FieldInfo):
    def __init__(
        self,
        vector_type: str | None = None,
        dimensions: int | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.vector_type = vector_type
        self.dimensions = dimensions


class ClassField:
    def __init__(self, model_field: ModelField, base_class: BaseModel):
        self.model_field = model_field
        self.base_class = base_class
        self.attr_names = [model_field.alias]

    def resolve(self, obj: T) -> T | None:
        for attr_name in self.attr_names:
            if not obj:
                return obj
            if isinstance(obj, dict):
                get_attribute = lambda attr_name: obj[attr_name]
            else:
                get_attribute = lambda attr_name: getattr(obj, attr_name)

            if attr_name.endswith("[0]"):
                # If the attribute is iterable, we get its firts object
                obj = get_attribute(attr_name[: attr_name.rindex("[0]")])
                if not obj:
                    return obj
                obj = obj[0]
            else:
                # Otherwise just get it
                obj = get_attribute(attr_name)

        return obj

    def join_attrs(self, char: str = ".") -> str:
        return char.join(self.attr_names).replace("[0]", "")

    def __getattribute__(self, name: str) -> Any:
        if name in {
            "model_field",
            "base_class",
            "attr_names",
            "resolve",
            "join_attrs",
        }:
            return super().__getattribute__(name)

        try:
            annotation = _get_unwrapped_annotation(self.model_field.annotation)
            base_class = _get_type_from_annotation(annotation, self.base_class)

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
        self.attr_names[-1] = f"{self.attr_names[-1]}[0]"
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.attr_names})"


def _get_unwrapped_annotation(annotation: T) -> T:
    annotation = _unwrap_optional(annotation)
    annotation = _unwrap_iterable(annotation)
    return annotation


def _unwrap_optional(annotation: T) -> T:
    if hasattr(annotation, "_name") and annotation._name == "Optional":
        return annotation.__args__[0]
    return annotation


def _unwrap_iterable(annotation: T) -> T:
    if hasattr(annotation, "__args__"):
        if annotation.__origin__ not in {list, set}:
            raise ValueError("Are you trying to outsmart me!? HA HA")
        return annotation.__args__[0]
    return annotation


def _get_type_from_annotation(annotation: T, base_class: BaseModel) -> BaseModel:
    if isinstance(annotation, ForwardRef):
        if not annotation.__forward_evaluated__:
            base_class.update_forward_refs()
        return annotation.__forward_value__
    return annotation
