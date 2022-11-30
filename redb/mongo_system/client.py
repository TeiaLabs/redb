from typing import Sequence, TypeVar

from pymongo import MongoClient as PymongoClient

from ..interfaces import (
    Client,
    Collection,
    Database,
)

from .database import MongoDatabase

T = TypeVar("T", bound=Collection)


class MongoClient(Client):
    def __init__(
        self, db_uri: str, default_database: str | None = None, **kwargs
    ) -> None:
        self.client = PymongoClient(db_uri, **kwargs)
        if default_database is None:
            self.default_database = MongoDatabase(self.client.get_default_database())
        else:
            self.default_database = self.get_database(default_database)

    def get_databases(self) -> Sequence[Database]:
        return [MongoDatabase(database) for database in self.client.list_databases()]

    def get_database(self, name: str) -> MongoDatabase:
        return MongoDatabase(self.client.get_database(name))

    def get_default_database(self) -> Database:
        return self.default_database

    def drop_database(self, name: str) -> None:
        self.client.drop_database(name)

    def close(self) -> None:
        self.client.close()
