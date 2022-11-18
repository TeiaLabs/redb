from typing import Any, Type, TypeVar

from ..instance import execute_collection_function
from ..interfaces import (
    BulkWriteResult,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
)

T = TypeVar("T")


class InsertionMixin:
    @classmethod
    def bulk_write(
        cls: Type[T], operations: list[PyMongoOperations]
    ) -> BulkWriteResult:
        return execute_collection_function(cls, "bulk_write", operations=operations)

    @classmethod
    def insert_one(cls: Type[T], data: Type[T]) -> InsertOneResult:
        return execute_collection_function(cls, "insert_one", data=data)

    def insert_one(self) -> InsertOneResult:
        return execute_collection_function(self.__class__, "insert_one", data=self)

    @classmethod
    def insert_many(cls: Type[T], data: list[Type[T]]) -> InsertManyResult:
        return execute_collection_function(cls, "insert_many", data=data)

    @classmethod
    def insert_vectors(cls: Type[T], data: dict[str, list[Any]]) -> list[T]:
        return execute_collection_function(cls, "insert_vectors", data=data)
