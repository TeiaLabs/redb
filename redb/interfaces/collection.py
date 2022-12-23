from abc import ABC, abstractmethod
from typing import Any, Type, TypeVar, Union

from pydantic import BaseModel
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)

from .fields import IncludeDBColumn, SortDBColumn, CompoundIndice
from .results import (
    BulkWriteResult,
    DeleteManyResult,
    DeleteOneResult,
    InsertManyResult,
    InsertOneResult,
    ReplaceOneResult,
    UpdateManyResult,
    UpdateOneResult,
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


class Collection(ABC, BaseModel):
    @staticmethod
    @abstractmethod
    def _get_driver_collection(instance_or_class: Type[T] | T) -> "Collection":
        pass

    @classmethod
    @abstractmethod
    def create_indice(cls: Type[T], indice: CompoundIndice) -> None:
        pass

    @classmethod
    @abstractmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeDBColumn] | list[str] | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
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
        sort: list[SortDBColumn] | SortDBColumn | None = None,
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
