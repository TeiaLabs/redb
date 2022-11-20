from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Type, TypeVar, Union

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
    result: dict[str, Any]
    upserted_id: Any


@dataclass
class UpdateManyResult(UpdateOneResult):
    pass


@dataclass
class ReplaceOneResult(UpdateOneResult):
    pass


class DeleteOneResult:
    pass


@dataclass
class DeleteManyResult:
    deleted_count: int


@dataclass
class InsertManyResult:
    inserted_ids: list[Any]


@dataclass
class InsertOneResult:
    inserted_id: Any


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

T = TypeVar("T")


class Collection(ABC):
    @abstractclassmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    @abstractclassmethod
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    @abstractclassmethod
    def find_one(
        cls: Type[T],
        filter: T | None = None,
        skip: int = 0,
    ) -> T:
        pass

    @abstractclassmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        pass

    @abstractclassmethod
    def count_documents(
        cls: Type[T],
        filter: T | None = None,
    ) -> int:
        pass

    @abstractclassmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    @abstractclassmethod
    def insert_one(
        cls: Type[T],
        data: T,
    ) -> InsertOneResult:
        pass

    def insert_one(self) -> InsertOneResult:
        pass

    @abstractclassmethod
    def insert_vectors(
        cls: Type[T],
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        pass

    @abstractclassmethod
    def insert_many(
        cls: Type[T],
        data: list[T],
    ) -> InsertManyResult:
        pass

    @abstractclassmethod
    def replace_one(
        cls: Type[T],
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        pass

    def replace_one(
        self,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        pass

    @abstractclassmethod
    def update_one(
        cls: Type[T],
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        pass

    def update_one(
        self,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        pass

    @abstractclassmethod
    def update_many(
        cls: Type[T],
        filter: T,
        update: list[T] | T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        pass

    @abstractclassmethod
    def delete_one(
        cls: Type[T],
        filter: T,
    ) -> DeleteOneResult:
        pass

    def delete_one(self) -> DeleteOneResult:
        pass

    @abstractclassmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        pass


class Database(ABC):
    @abstractmethod
    def get_collections(self) -> list[Collection]:
        pass

    @abstractmethod
    def get_collection(cls, name: str) -> Collection:
        pass

    @abstractmethod
    def create_collection(cls, name: str) -> None:
        pass

    @abstractmethod
    def delete_collection(cls, name: str) -> None:
        pass


class Client(ABC):
    @abstractmethod
    def get_default_database(self) -> Database:
        pass

    @abstractmethod
    def get_databases(self) -> list[Database]:
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
