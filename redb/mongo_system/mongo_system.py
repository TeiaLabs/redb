from typing import Any

from pymongo import MongoClient as PymongoClient
from pymongo.database import Database as PymongoDatabase

from ..interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteManyResult,
    DeleteOneResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    SortField,
    UpdateManyResult,
    UpdateOneResult,
)


class MongoClient(Client):
    def __init__(
        self, db_uri: str, default_database: str | None = None, **kwargs
    ) -> None:
        self.client: PymongoClient = PymongoClient(db_uri, **kwargs)
        if default_database is None:
            self.default_database = MongoDatabase(self.client.get_default_database())
        else:
            self.default_database = self.get_database(default_database)

    def get_databases(self) -> list[Database]:
        return [MongoDatabase(database) for database in self.client.list_databases()]

    def get_database(self, name: str) -> Database:
        return MongoDatabase(self.client.get_database(name))

    def get_default_database(self) -> Database:
        return self.default_database

    def drop_database(self, name: str) -> None:
        self.client.drop_database(name)

    def close(self) -> None:
        self.client.close()


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.database = database

    def get_collections(self) -> list[Collection]:
        return [
            MongoCollection(collection)
            for collection in self.database.list_collections()
        ]

    def get_collection(self, name: str) -> Collection:
        return MongoCollection(self.database[name])

    def create_collection(self, name: str) -> None:
        self.database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.database.drop_collection(name)

    def __getitem__(self, name) -> Collection:
        return MongoCollection(self.database[name])


class MongoCollection(Collection):
    pass
