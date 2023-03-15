from typing import Sequence

from migo.client import Client as MigoDriverClient

from redb.interface.client import Client
from redb.interface.configs import MigoConfig

from .database import MigoDatabase


class MigoClient(Client):
    def __init__(self, migo_config: MigoConfig | dict):
        migo_config.mongo_kwargs["host"] = migo_config.mongo_database_uri
        migo_config.milvus_kwargs["alias"] = migo_config.milvus_connection_alias
        migo_config.milvus_kwargs["host"] = migo_config.milvus_host
        migo_config.milvus_kwargs["port"] = migo_config.milvus_port

        self.__migo_driver = MigoDriverClient(
            migo_config.mongo_kwargs, migo_config.milvus_kwargs
        )

    def _get_driver_client(self) -> MigoDriverClient:
        return self.__migo_driver

    def get_default_database(self) -> MigoDatabase:
        return MigoDatabase(self.__migo_driver.get_default_database())

    def get_database(self, name: str) -> MigoDatabase:
        return MigoDatabase(self.__migo_driver.get_database(name))

    def get_databases(self) -> Sequence[MigoDatabase]:
        return [
            MigoDatabase(database) for database in self.__migo_driver.get_databases()
        ]

    def get_database(self, name: str) -> MigoDatabase:
        return MigoDatabase(self.__migo_driver.get_database(name))

    def drop_database(self, name: str) -> bool:
        try:
            self.__migo_driver.drop_database(name)
            return True
        except:
            return False

    def close(self) -> bool:
        try:
            self.__migo_driver.close()
            return True
        except:
            return False
