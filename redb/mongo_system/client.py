from typing import Sequence

from pymongo import MongoClient as PymongoClient

from ..interfaces import Client
from .config import MongoConfig
from .database import MongoDatabase


class MongoClient(Client):
    def __init__(self, mongo_config: MongoConfig) -> None:
        self.client = PymongoClient(
            mongo_config.database_uri, **mongo_config.driver_kwargs
        )
        if mongo_config.default_database is None:
            self.default_database = MongoDatabase(
                self.client.get_default_database()
            )
        else:
            self.default_database = self.get_database(mongo_config.default_database)

    def get_databases(self) -> Sequence[MongoDatabase]:

    def get_database(self, name: str) -> MongoDatabase:
        return MongoDatabase(self.client.get_database(name))

    def get_default_database(self) -> MongoDatabase:
        return self.default_database

    def drop_database(self, name: str) -> None:
        self.client.drop_database(name)

    def close(self) -> None:
        self.client.close()
