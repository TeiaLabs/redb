from typing import Type, TypeVar

from ..instance import execute_collection_function
from ..interfaces import IncludeField, SortField

T = TypeVar("T")


class RetrievalMixin:
    @classmethod
    def find(
        cls: Type[T],
        filter: Type[T] | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        return execute_collection_function(
            cls,
            "find",
            filter=filter,
            fields=fields,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    @classmethod
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: Type[T] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        return execute_collection_function(
            cls,
            "find_vectors",
            column=column,
            filter=filter,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    @classmethod
    def find_one(cls: Type[T], filter: Type[T] | None = None, skip: int = 0) -> T:
        return execute_collection_function(cls, "find_one", filter=filter, skip=skip)
