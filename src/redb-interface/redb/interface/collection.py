from abc import ABC, abstractmethod
from typing import Any, TypeVar, Union

from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)

from .fields import CompoundIndice, IncludeColumn, SortColumn
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

T = TypeVar("T", bound=dict[str, Any])


class Collection(ABC):
    @abstractmethod
    def _get_driver_collection(self) -> "Collection":
        pass

    @abstractmethod
    def create_indice(self, indice: CompoundIndice) -> None:
        pass

    @abstractmethod
    def find(
        self,
        filter: T | None = None,
        fields: list[IncludeColumn] | list[str] | None = None,
        sort: list[SortColumn] | SortColumn | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[T]:
        pass

    @abstractmethod
    def find_vectors(
        self,
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortColumn] | SortColumn | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[T]:
        pass

    @abstractmethod
    def find_one(self, filter: T | None = None, skip: int = 0) -> T:
        pass

    @abstractmethod
    def distinct(self, key: str, filter: T | None = None) -> list[T]:
        pass

    @abstractmethod
    def count_documents(self, filter: T | None = None) -> int:
        pass

    @abstractmethod
    def bulk_write(self, operations: list[PyMongoOperations]) -> BulkWriteResult:
        pass

    @abstractmethod
    def insert_one(self, data: T) -> InsertOneResult:
        pass

    @abstractmethod
    def insert_vectors(sel, data: dict[str, list[Any]]) -> InsertManyResult:
        pass

    @abstractmethod
    def insert_many(self, data: list[T]) -> InsertManyResult:
        pass

    @abstractmethod
    def replace_one(
        self,
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        pass

    @abstractmethod
    def update_one(
        self,
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        pass

    @abstractmethod
    def update_many(
        self,
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        pass

    @abstractmethod
    def delete_one(self, filter: T) -> DeleteOneResult:
        pass

    @abstractmethod
    def delete_many(self, filter: T) -> DeleteManyResult:
        pass
