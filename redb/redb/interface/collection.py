from abc import ABC, abstractmethod
from typing import Any, Type, TypeAlias

from redb.core import BaseDocument

from .fields import CompoundIndex, PyMongoOperations
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

Json: TypeAlias = dict[str, Any]
OptionalJson: TypeAlias = dict[str, Any] | None
ReturnType: TypeAlias = BaseDocument | dict


class Collection(ABC):
    @abstractmethod
    def _get_driver_collection(self) -> "Collection":
        pass

    @abstractmethod
    def create_index(
        self,
        index: CompoundIndex,
    ) -> bool:
        pass

    @abstractmethod
    def find(
        self,
        cls: Type[BaseDocument],
        return_cls: Type[ReturnType],
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        sort: list[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        pass

    @abstractmethod
    def find_one(
        self,
        cls: Type[BaseDocument],
        return_cls: Type[ReturnType],
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        skip: int = 0,
    ) -> ReturnType:
        pass

    @abstractmethod
    def distinct(
        self,
        cls: ReturnType,
        key: str,
        filter: OptionalJson = None,
    ) -> list[Any]:
        pass

    @abstractmethod
    def count_documents(
        self,
        cls: Type[BaseDocument],
        filter: OptionalJson = None,
    ) -> int:
        pass

    @abstractmethod
    def bulk_write(
        self,
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    @abstractmethod
    def insert_one(
        self,
        cls: Type[BaseDocument],
        data: Json,
    ) -> InsertOneResult:
        pass

    @abstractmethod
    def insert_many(
        self,
        cls: Type[BaseDocument],
        data: list[Json],
    ) -> InsertManyResult:
        pass

    @abstractmethod
    def replace_one(
        self,
        cls: Type[BaseDocument],
        filter: Json,
        replacement: Json,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        pass

    @abstractmethod
    def update_one(
        self,
        cls: Type[BaseDocument],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateOneResult:
        pass

    @abstractmethod
    def update_many(
        self,
        cls: Type[BaseDocument],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateManyResult:
        pass

    @abstractmethod
    def delete_one(
        self,
        cls: Type[BaseDocument],
        filter: Json,
    ) -> DeleteOneResult:
        pass

    @abstractmethod
    def delete_many(
        self,
        cls: Type[BaseDocument],
        filter: Json,
    ) -> DeleteManyResult:
        pass
