from typing import Any, Type, TypeVar

from ..instance import execute_collection_function
from ..interfaces import (
    BulkWriteResult,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
)
from .base import get_collection

T = TypeVar("T")


class InsertionMixin:
    @classmethod
    def bulk_write(
        cls: Type[T], operations: list[PyMongoOperations]
    ) -> BulkWriteResult:
        collection = get_collection(cls)
        return collection.bulk_write(operations)

    @classmethod
    def insert_one(cls: Type[T], data: Type[T]) -> InsertOneResult:
        collection = get_collection(cls)
        return collection.insert_one(data)

    def insert_one(self) -> InsertOneResult:
        collection = get_collection(self.__class__)
        return collection.insert_one(self)

    @classmethod
    def insert_many(cls: Type[T], data: list[Type[T]]) -> InsertManyResult:
        collection = get_collection(cls)
        return collection.insert_many(data)

    @classmethod
    def insert_vectors(cls: Type[T], data: dict[str, list[Any]]) -> list[T]:
        collection = get_collection(cls)
        return collection.insert_vectors(data)
