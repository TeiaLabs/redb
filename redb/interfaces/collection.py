import hashlib
import pickle
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

from .fields import IncludeField, SortField
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
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
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
        sort: list[SortField] | SortField | None = None,
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


class BaseCollection(Collection, BaseModel):
    __database_name__: str | None = None
    __client_name__: str | None = None

    hash: str | None = None

    def __init__(self, collection_name: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        object.__setattr__(
            self, "__collection_name__", collection_name or self.__class__.__name__
        )

    @classmethod
    def collection_name(cls: Type[T]) -> str:
        return cls.__name__.lower()

    def get_hash(self) -> str:
        hashses = []
        for field in self.__fields__:
            if field == "hash":
                continue
            value = self.__getattribute__(field)
            key_field_hash = hashlib.md5(field.encode("utf8")).hexdigest()
            val_field_hash = hashlib.md5(pickle.dumps(value)).hexdigest()
            hashses += [key_field_hash, val_field_hash]

        hex_digest = hashlib.sha256("".join(hashses).encode("utf-8")).hexdigest()
        return hex_digest

    def dict(self, *args, **kwargs) -> dict:
        exclude_unset = kwargs.pop("exclude_unset", True)
        data = super().dict(*args, exclude_unset=exclude_unset, **kwargs)
        
        data_hash = self.get_hash()
        if data_hash is not None:
            data["hash"] = data_hash

        return data

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"
