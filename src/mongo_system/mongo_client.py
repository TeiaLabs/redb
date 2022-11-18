from typing import Any

from ..redb.interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    JSONDocument,
    PyMongoOperations,
    SortField,
    UpdateResult,
)


class MongoCollection(Collection):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    def find(
        self,
        filter: JSONDocument | None,
        fields: list[IncludeField] | list[str] | None,
        sort: list[SortField] | SortField | None,
        skip: int | None,
        limit: int | None,
    ) -> list[JSONDocument]:
        pass

    def find_vectors(
        self,
        column: str,
        filter: JSONDocument | None,
        sort: list[SortField] | SortField | None,
        skip: int | None,
        limit: int | None,
    ) -> list[JSONDocument]:
        pass

    def find_one(self, filter: JSONDocument | None, skip: int | None) -> JSONDocument:
        pass

    def distinct(self, key: str, filter: JSONDocument | None) -> list[Any]:
        pass

    def count_documents(self, filter: JSONDocument | None) -> int:
        pass

    def bulk_write(self, operations: list[PyMongoOperations]) -> BulkWriteResult:
        pass

    def insert_one(self, data: JSONDocument) -> InsertOneResult:
        pass

    def insert_vectors(self, data: dict[str, list[Any]]) -> InsertManyResult:
        pass

    def insert_many(self, data: list[JSONDocument]) -> InsertManyResult:
        pass

    def replace_one(
        self, filter: JSONDocument, replacement: JSONDocument, upsert: bool
    ) -> UpdateResult:
        pass

    def update_one(
        self, filter: JSONDocument, update: JSONDocument, upsert: bool
    ) -> UpdateResult:
        pass

    def update_many(
        self,
        filter: JSONDocument,
        update: list[JSONDocument] | JSONDocument,
        upsert: bool,
    ) -> UpdateResult:
        pass

    def delete_one(self, filter: JSONDocument) -> DeleteResult:
        pass

    def delete_many(self, filter: JSONDocument) -> DeleteResult:
        pass


class MongoDatabase(Database):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    def get_collections(self) -> list[Collection]:
        return [MongoCollection()]

    def get_collection(self, name: str) -> Collection:
        return MongoCollection()

    def create_collection(self, name: str) -> None:
        return None

    def delete_collection(self, name: str) -> None:
        return None

    def __getitem__(self, name) -> Database:
        return MongoCollection()


class MongoClient(Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    def get_databases(self) -> list[Database]:
        return [MongoDatabase()]

    def get_database(self, name: str) -> Database:
        return MongoDatabase()

    def get_default_database(self) -> Database:
        return MongoDatabase()

    def drop_database(self, name: str) -> None:
        return None

    def close() -> None:
        pass

    def __getitem__(self, name) -> Database:
        return MongoDatabase()
