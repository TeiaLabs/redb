from migo.database import Database as MigoDriverDatabase

from redb.interface.database import Database

from .collection import MigoCollection


class MigoDatabase(Database):
    def __init__(self, database: MigoDriverDatabase) -> None:
        self.__database = database

    def _get_driver_database(self) -> MigoDriverDatabase:
        return self.__database

    def get_collections(self) -> list[MigoCollection]:
        return [
            MigoCollection(collection)
            for collection in self.__database.get_collections()
        ]

    def get_collection(self, name: str) -> MigoCollection:
        return MigoCollection(self.__database.get_collection(name))

    def create_collection(self, name: str) -> bool:
        try:
            self.__database.create_collection(name)
            return True
        except:
            return False

    def delete_collection(self, name: str) -> bool:
        try:
            self.__database.delete_collection(name)
            return True
        except:
            return False
        
    @property
    def name(self) -> str:
        return self.__database.name

    def __getitem__(self, name: str) -> MigoCollection:
        return self.get_collection(name)
