from abc import abstractclassmethod, abstractmethod
from typing import Any, TypeVar, Type

import pydantic

from . import init_db

T = TypeVar("T")


class InsertionMixin:
    @abstractclassmethod
    def insert_one(cls: Type[T], data: dict[str, Any]) -> T:
        ...

    @abstractclassmethod
    def insert_many(cls: Type[T], data: list[dict[str, Any]]) -> list[T]:
        ...

    @abstractclassmethod
    def insert_vectors(cls: Type[T], data: dict[str, list[Any]]) -> list[T]:
        """
        Insert a batch of vectors into the database.

        :param data: dict of field name and columnar lists of values.
        """
        ...

    @abstractmethod
    def insert(self):
        return init_db.REDB.get_doc_class().insert(self)


class RetrievalMixin:
    @abstractclassmethod
    def find_one(cls: Type[T], filters: dict | None = None) -> T:
        ...

    @abstractclassmethod
    def find_many(
        cls: Type[T],
        filters: dict | None = None,
        pagination: slice | None = None,
        projection: dict | None = None,
        sorting: dict | None = None,
    ) -> list[T]:
        ...

    @abstractclassmethod
    def find_vectors(
        cls,
        batch_size: int | None = None,
        filters: dict | None = None,
        projection: dict | None = None,
        sorting: dict | None = None,
    ) -> dict[str, list[Any]]:
        ...

    @abstractclassmethod
    def find_by_id(cls: Type[T], id: str) -> T:
        ...


class Document(pydantic.BaseModel, InsertionMixin, RetrievalMixin):

    @property
    def collection_name(self) -> str:
        return self.__class__.__name__.lower()
