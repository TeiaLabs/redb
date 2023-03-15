from pymongo.database import Database as PymongoDatabase
from pymongo.errors import CollectionInvalid

from redb.interface.database import Database

from .collection import MongoCollection


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.__database = database

    def _get_driver_database(self) -> PymongoDatabase:
        return self.__database

    def get_collections(self) -> list[MongoCollection]:
        return [
            MongoCollection(self.__database[col["name"]])
            for col in self.__database.list_collections()
        ]

    def get_collection(self, name: str) -> MongoCollection:
        return MongoCollection(self.__database[name])

    def create_collection(self, name: str) -> bool:
        try:
            self.__database.create_collection(name)
            return True
        except CollectionInvalid:
            return False

    def delete_collection(self, name: str) -> bool:
        try:
            self.__database.drop_collection(name)
            return True
        except CollectionInvalid:
            return False
        
    @property
    def name(self) -> str:
        return self.__database.name

    def __getitem__(self, name: str) -> MongoCollection:
        return self.get_collection(name)
