from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TypeVar, Union, Mapping, Any

import pymongo
from pydantic import BaseModel
from pymongo.operations import (
    InsertOne,
    DeleteOne,
    DeleteMany,
    ReplaceOne,
    UpdateOne,
    UpdateMany,
)
from pymongo.results import (
    InsertOneResult,
    InsertManyResult,
    BulkWriteResult,
    DeleteResult,
    UpdateResult,
)


class Direction(Enum):
    ASCENDING = pymongo.ASCENDING
    DESCENGIND = pymongo.DESCENDING


class Field(BaseModel):
    name: str


class IncludeField(Field):
    include: bool


class SortField(Field):
    direction: Direction


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

JSONDocument = TypeVar("JSONDocument", bound=Mapping[str, Any])


class Collection(ABC):
    @abstractmethod
    def find(
        self,
        filter: JSONDocument | None,
        fields: list[IncludeField] | list[str] | None,
        sort: list[SortField] | SortField | None,
        skip: int | None,
        limit: int | None,
    ) -> list[JSONDocument]:
        pass

    @abstractmethod
    def find_colunary(
        self,
        column: str,
        filter: JSONDocument | None,
        sort: list[SortField] | SortField | None,
        skip: int | None,
        limit: int | None,
    ) -> list[JSONDocument]:
        pass

    @abstractmethod
    def find_one(self, filter: JSONDocument | None, skip: int | None) -> JSONDocument:
        pass

    @abstractmethod
    def distinct(self, key: str, filter: JSONDocument | None) -> list[Any]:
        pass

    @abstractmethod
    def count_documents(self, filter: JSONDocument | None) -> int:
        pass

    @abstractmethod
    def bulk_write(self, operations: list[PyMongoOperations]) -> BulkWriteResult:
        pass

    @abstractmethod
    def insert_one(self, data: JSONDocument) -> InsertOneResult:
        pass

    @abstractmethod
    def insert_many(self, data: list[JSONDocument]) -> InsertManyResult:
        pass

    @abstractmethod
    def replace_one(
        self, filter: JSONDocument, replacement: JSONDocument, upsert: bool
    ) -> UpdateResult:
        pass

    @abstractmethod
    def update_one(
        self, filter: JSONDocument, update: JSONDocument, upsert: bool
    ) -> UpdateResult:
        pass

    @abstractmethod
    def update_many(
        self,
        filter: JSONDocument,
        update: list[JSONDocument] | JSONDocument,
        upsert: bool,
    ) -> UpdateResult:
        pass

    @abstractmethod
    def delete_one(self, filter: JSONDocument) -> DeleteResult:
        pass

    @abstractmethod
    def delete_many(self, filter: JSONDocument) -> DeleteResult:
        pass


class Database(ABC):
    @abstractmethod
    def get_collections(self) -> list[Collection]:
        pass

    @abstractmethod
    def get_collection(self, name: str) -> Collection:
        pass

    @abstractmethod
    def create_collection(self, name: str) -> None:
        pass

    @abstractmethod
    def delete_collection(self, name: str) -> None:
        pass


class Client(ABC):
    @abstractmethod
    def get_databases(self) -> list[Database]:
        pass

    @abstractmethod
    def get_database(self, name: str) -> Database:
        pass

    @abstractmethod
    def drop_database(self, name: str) -> None:
        pass

    @abstractmethod
    def close() -> None:
        pass
