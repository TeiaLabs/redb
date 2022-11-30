from typing import TypeVar

from pymongo.database import Database as PymongoDatabase

from ..interfaces import (
    Collection,
    Database,
)

from .collection import MongoCollection

T = TypeVar("T", bound=Collection)


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.database = database

    def get_collections(self) -> list[Collection]:
        return [collection for collection in self.database.list_collections()]

    def get_collection(self, name: str) -> Collection:
        return MongoCollection(self.database[name])

    def create_collection(self, name: str) -> None:
        self.database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.database.drop_collection(name)

    def __getitem__(self, name) -> Collection:
        return self.database[name]
