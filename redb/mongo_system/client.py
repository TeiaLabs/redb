from typing import Sequence

from pymongo import MongoClient as PymongoClient

from ..interfaces import Client
from .config import MongoConfig
from .database import MongoDatabase


class MongoClient(Client):
    def __init__(self, mongo_config: MongoConfig) -> None:
        self.__client = PymongoClient(
            mongo_config.database_uri, **mongo_config.driver_kwargs
        )
        if mongo_config.default_database is None:
            self.__default_database = MongoDatabase(
                self.__client.get_default_database()
            )
        else:
            self.__default_database = self.get_database(mongo_config.default_database)

    def _get_driver_client(self) -> "Client":
        return self.__client

    def get_databases(self) -> Sequence[MongoDatabase]:
        return [
            MongoDatabase(self.__client.get_database(database["name"]))
            for database in self.__client.list_databases()
        ]

    def get_database(self, name: str) -> MongoDatabase:
        return MongoDatabase(self.__client.get_database(name))

    def get_default_database(self) -> MongoDatabase:
        return self.__default_database

    def drop_database(self, name: str) -> None:
        self.__client.drop_database(name)

    def close(self) -> None:
        self.__client.close()
