from dataclasses import dataclass
from enum import Enum
from types import UnionType
from typing import Any, ForwardRef, Literal, TypeVar, Union, _UnionGenericAlias

import pymongo
from bson import DBRef as BsonDBRef
from bson import ObjectId as BsonObjectId
from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from pydantic.fields import ModelField
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

    @classmethod
    def from_string(
        cls, query: Literal["asc", "desc", "2d", "sphere", "hashed", "text"]
    ) -> "Direction":
        if query == "asc":
            return cls.ASCENDING
        elif query == "desc":
            return cls.DESCENDING
        elif query == "2d":
            return cls.GEO2D
        elif query == "sphere":
            return cls.GEOSPHERE
        elif query == "hashed":
            return cls.HASHED
        elif query == "text":
            return cls.TEXT
        else:
            raise ValueError(f"Invalid sort order: {query}.")


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


class FieldInfo(PydanticFieldInfo):
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


def Field(*args, **kwargs) -> Any:
    return FieldInfo(*args, **kwargs)


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
            annotation = _get_unwrapped_annotation(self.model_field.outer_type_)
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
    annotation = _unwrap_union(annotation)
    annotation = _unwrap_iterable(annotation)
    annotation = _unwrap_union(annotation)
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


def _unwrap_union(annotation: T) -> T:
    if hasattr(annotation, "__args__") and annotation.__class__ in {
        UnionType,
        _UnionGenericAlias,
    }:
        return annotation.__args__[0]
    return annotation


def _get_type_from_annotation(annotation: T, base_class: BaseModel) -> BaseModel:
    if isinstance(annotation, ForwardRef):
        if not annotation.__forward_evaluated__:
            base_class.update_forward_refs()
        return annotation.__forward_value__
    return annotation


class ObjectId(BsonObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v) -> "ObjectId":
        if not BsonObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId.")
        return ObjectId(v)

    @classmethod
    def __modify_schema__(cls, field_schema) -> str:
        field_schema.update(type="string")

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self)}')"


class DBRefField(BaseModel):
    id: Any
    collection: str
    database: str | None = None

    class Config:
        json_encoders = {
            ObjectId: str,
        }

    def dict(self, *_, **__) -> dict:
        return {"$id": self.id, "$ref": self.collection, "$db": self.database}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.dict()}')"


class BsonDBRefField(BaseModel):
    id: Any = Field(alias="$id")
    ref: str = Field(alias="$ref")
    db: str | None = Field(None, alias="$db")


class DBRef(BsonDBRef):
    """Warning: does not work with openapi schema because of $ref."""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        field = v
        if isinstance(v, BsonDBRef):
            field = DBRefField(
                id=v.id,
                collection=v.collection,
                database=v.database,
            )
        elif isinstance(field, dict):
            if "$id" in field:
                tmp = BsonDBRefField(**field)
                field = DBRefField(
                    id=tmp.id,
                    collection=tmp.ref,
                    database=tmp.db,
                )
            else:
                field = DBRefField(**v)

        if not isinstance(field, DBRefField):
            raise ValueError(f"Cannot construct PyDBField from value: {v}")

        return field

    @classmethod
    def __modify_schema__(cls, _):
        return DBRefField

    def __repr__(self):
        return f"{self.__class__.__name__}('{dict(self.as_doc())}')"
