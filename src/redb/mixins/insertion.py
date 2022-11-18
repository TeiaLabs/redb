from abc import abstractclassmethod
from typing import Any, Type, TypeVar

from ..interfaces import (
    BulkWriteResult,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
)
from .base import Base

T = TypeVar("T")


class InsertionMixin(Base):
    @abstractclassmethod
    def bulk_write(
        cls: Type[T], operations: list[PyMongoOperations]
    ) -> BulkWriteResult:
        ...

    @abstractclassmethod
    def insert_one(cls: Type[T], data: Type[T]) -> InsertOneResult:
        ...

    def insert_one(self) -> InsertOneResult:
        ...

    @abstractclassmethod
    def insert_many(cls: Type[T], data: list[Type[T]]) -> InsertManyResult:
        ...

    @abstractclassmethod
    def insert_vectors(cls: Type[T], data: dict[str, list[Any]]) -> list[T]:
        ...
