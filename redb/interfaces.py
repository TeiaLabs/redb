import hashlib
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Sequence, Type, TypeVar, Union

import pymongo
from pydantic import BaseModel
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)

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

T = TypeVar("T", bound="Collection")


class Direction(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENGIND = pymongo.DESCENDING


class Field(BaseModel):
    name: str


class IncludeField(Field):
    include: bool


class SortField(Field):
    direction: Direction


@dataclass
class BulkWriteResult:
    deleted_count: int
    inserted_count: int
    matched_count: int
    modified_count: int
    upserted_count: int
    upserted_ids: int


@dataclass
class UpdateOneResult:
    matched_count: int
    modified_count: int
    upserted_id: Any


@dataclass
class UpdateManyResult(UpdateOneResult):
    pass


@dataclass
class ReplaceOneResult(UpdateOneResult):
    pass


@dataclass
class DeleteOneResult:
    deleted_count: int = 1

@dataclass
class DeleteManyResult:
    deleted_count: int


@dataclass
class InsertManyResult:
    inserted_ids: list[Any]


@dataclass
class InsertOneResult:
    inserted_id: Any


class Collection(ABC, BaseModel):
    __database_name__: str | None = None
    __client_name__: str | None = None
    __collection_name__: str | None = None

    @classmethod
    @abstractmethod
    def _get_driver_collection(cls: Type[T]) -> "Collection":
        pass

    @classmethod
    @abstractmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    @classmethod
    @abstractmethod
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    @classmethod
    @abstractmethod
    def find_one(
        cls: Type[T],
        filter: T | None = None,
        skip: int = 0,
    ) -> T:
        pass

    @classmethod
    @abstractmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        pass

    @classmethod
    @abstractmethod
    def count_documents(
        cls: Type[T],
        filter: T | None = None,
    ) -> int:
        pass

    @classmethod
    @abstractmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    @abstractmethod
    def insert_one(data: T) -> InsertOneResult:
        pass

    @classmethod
    @abstractmethod
    def insert_vectors(
        cls: Type[T],
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        pass

    @classmethod
    @abstractmethod
    def insert_many(
        cls: Type[T],
        data: list[T],
    ) -> InsertManyResult:
        pass

    @abstractmethod
    def replace_one(
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        pass

    @abstractmethod
    def update_one(
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        pass

    @classmethod
    @abstractmethod
    def update_many(
        cls: Type[T],
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        pass

    @abstractmethod
    def delete_one(self, filter: T) -> DeleteOneResult:
        pass

    @classmethod
    @abstractmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        pass

    @classmethod
    def collection_name(cls) -> str:
        return cls.__name__.lower()

    def get_hash(self) -> str:
        hashses = []
        for field in self.__fields__:
            value = self.__getattribute__(field)
            key_field_hash = hashlib.md5(field.encode("utf8")).hexdigest()
            val_field_hash = hashlib.md5(pickle.dumps(value)).hexdigest()
            hashses += [key_field_hash, val_field_hash]

        hex_digest = hashlib.sha256("".join(hashses).encode("utf-8")).hexdigest()
        return hex_digest

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"


class Database(ABC):
    @abstractmethod
    def _get_driver_database(self) -> "Database":
        pass

    @abstractmethod
    def get_collections(self) -> list[Collection]:
        pass

    @abstractmethod
    def get_collection(self, name: str) -> Collection:
        pass

    @abstractmethod
    def create_collection(self, name: str) -> None:
        pass

    @abstractmethod
    def delete_collection(self, name: str) -> None:
        pass

    @abstractmethod
    def __getitem__(self, name) -> Collection:
        pass


class Client(ABC):
    @abstractmethod
    def _get_driver_client(self) -> "Client":
        pass

    @abstractmethod
    def get_default_database(self) -> Database:
        pass

    @abstractmethod
    def get_databases(self) -> Sequence[Database]:
        pass

    @abstractmethod
    def get_database(self, name: str) -> Database:
        pass

    @abstractmethod
    def drop_database(self, name: str) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


Collection.update_forward_refs()
