from pymongo.database import Database as PymongoDatabase

from ..interfaces import Database
from .collection import MongoCollection


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.__database = database

    def _get_driver_database(self) -> "Database":
        return self.__database

    def get_collections(self) -> list[MongoCollection]:
        return list(self.__database.list_collections())

    def get_collection(self, name: str) -> MongoCollection:
        return MongoCollection.__new__(MongoCollection, name)

    def create_collection(self, name: str) -> None:
        self.__database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.__database.drop_collection(name)

    def __getitem__(self, name) -> MongoCollection:
        return self.get_collection(name)
