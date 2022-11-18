from typing import Any, Type, TypeVar

from ..redb.interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    SortField,
    UpdateResult,
)
from ..redb.mixins import Document

T = TypeVar("T", bound=Document)


class JSONClient(Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    def get_databases(self) -> list[Database]:
        return [JSONDatabase()]

    def get_database(self, name: str) -> Database:
        return JSONDatabase()

    def get_default_database(self) -> Database:
        return JSONDatabase()

    def drop_database(self, name: str) -> None:
        return None

    def close() -> None:
        pass


class JSONDatabase(Database):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    def get_collections(self) -> list[Collection]:
        return [JSONCollection()]

    def get_collection(self, name: str) -> Collection:
        return JSONCollection()

    def create_collection(self, name: str) -> None:
        return None

    def delete_collection(self, name: str) -> None:
        return None

    def __getitem__(self, name) -> Database:
        return JSONCollection()

    def get_client(self) -> Client:
        return self.client


class JSONCollection(Document, Collection):
    __client_name__ = "json"

    @classmethod
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
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        collection = get_collection(cls)

    @classmethod
    def find_one(
        cls: Type[T],
        filter: T | None = None,
        skip: int = 0,
    ) -> T:
        pass

    @classmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        pass

    @classmethod
    def count_documents(
        cls: Type[T],
        filter: T | None = None,
    ) -> int:
        pass

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    @classmethod
    def insert_one(
        cls: Type[T],
        data: T,
    ) -> InsertOneResult:
        pass

    def insert_one(self) -> InsertOneResult:
        pass

    @classmethod
    def insert_vectors(
        cls: Type[T],
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        pass

    @classmethod
    def insert_many(
        cls: Type[T],
        data: list[T],
    ) -> InsertManyResult:
        pass

    @classmethod
    def replace_one(
        cls: Type[T],
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> UpdateResult:
        pass

    def replace_one(
        self,
        replacement: T,
        upsert: bool = False,
    ) -> UpdateResult:
        pass

    @classmethod
    def update_one(
        cls: Type[T],
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateResult:
        pass

    def update_one(
        self,
        update: T,
        upsert: bool = False,
    ) -> UpdateResult:
        pass

    @classmethod
    def update_many(
        cls: Type[T],
        filter: T,
        update: list[T] | T,
        upsert: bool = False,
    ) -> UpdateResult:
        pass

    @classmethod
    def delete_one(
        cls: Type[T],
        filter: T,
    ) -> DeleteResult:
        pass

    def delete_one(self) -> DeleteResult:
        pass

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteResult:
        pass


def get_collection(cls: Type[JSONCollection]) -> Collection:
    from ..redb.instance import RedB

    client = RedB.get_client(cls.__client_name__)
    database = (
        client.get_database(cls.__database_name__)
        if cls.__database_name__
        else client.get_default_database()
    )
    return database.get_collection(cls.__class__.__name__)
