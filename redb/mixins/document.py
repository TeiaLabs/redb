from abc import abstractclassmethod, abstractmethod
from typing import Any, TypeVar, Type, Optional
import hashlib
import pickle

import pydantic

from .. import init_db

T = TypeVar("T")


class InsertionMixin:
    @abstractclassmethod
    def insert_one(cls: Type[T], data: dict[str, Any]) -> T:
        ...

    @abstractclassmethod
    def insert_many(cls: Type[T], data: list[dict[str, Any]]) -> list[T]:
        init_db.REDB.get_doc_class().insert_many(cls, data)

    @abstractclassmethod
    def insert_vectors(cls: Type[T], data: dict[str, list[Any]]) -> list[T]:
        """
        Insert a batch of vectors into the database.

        :param data: dict of field name and columnar lists of values.
        """
        init_db.REDB.get_doc_class().insert_vectors(cls, data)

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
        return init_db.REDB.get_doc_class().find_many(cls)

    @abstractclassmethod
    def find_vectors(
        cls,
        batch_size: int | None = None,
        filters: dict | None = None,
        projection: dict | None = None,
        sorting: dict | None = None,
    ) -> dict[str, list[Any]]:
        return init_db.REDB.get_doc_class().find_vectors(cls)

    @abstractclassmethod
    def find_by_id(cls: Type[T], id: str) -> T:
        return init_db.REDB.get_doc_class().find_by_id(cls, id)


class Document(pydantic.BaseModel, InsertionMixin, RetrievalMixin):

    @classmethod
    def collection_name(cls) -> str:
        return cls.__name__.lower()

    def __repr__(self) -> str:
        r = self.__class__.__name__
        r += "("
        # add all pydantic attributes like attr=val, attr2=val2
        r += ", ".join(f"{f}={getattr(self, f)}" for f in self.__fields__)
        r += ")"
        return r

    def get_hash(self, fields) -> str:
        hashses = []
        for field in fields:
            value = self.__getattribute__(field)
            key_field_hash = self.hash_function(field.encode('utf8'))
            val_field_hash = self.hash_function(pickle.dumps(value))
            hashses += [key_field_hash, val_field_hash]

        hex_digest = hashlib.sha256("".join(hashses).encode("utf-8")).hexdigest()
        return hex_digest

    @staticmethod
    def hash_function(buf):
        return hashlib.md5(buf).hexdigest()
