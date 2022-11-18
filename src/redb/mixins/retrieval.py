from abc import abstractclassmethod
from typing import Type, TypeVar

from ..interfaces import IncludeField, SortField

T = TypeVar("T")


class RetrievalMixin:
    @abstractclassmethod
    def find(
        cls: Type[T],
        filter: Type[T] | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        ...

    @abstractclassmethod
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: Type[T] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        ...

    @abstractclassmethod
    def find_one(cls: Type[T], filter: Type[T] | None = None, skip: int = 0) -> T:
        ...
