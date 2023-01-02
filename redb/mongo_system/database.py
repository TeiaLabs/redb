from pymongo.database import Database as PymongoDatabase

from ..interfaces import Database
from .collection import Collection


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.__database = database

    def _get_driver_database(self) -> "Database":
        return self.__database

    def get_collections(self) -> list[Collection]:
        return [
            self.__database[col["name"]] for col in self.__database.list_collections()
        ]

    def get_collection(self, name: str) -> Collection:
        return self.__database[name]

    def create_collection(self, name: str) -> None:
        self.__database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.__database.drop_collection(name)

    def __getitem__(self, name) -> Collection:
        return self.get_collection(name)
