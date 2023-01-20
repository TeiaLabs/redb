from typing import Sequence

from migo.client import Client as MigoDriverClient

from .database import MigoDatabase
from redb.interface.configs import MigoConfig


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

    def get_databases(self) -> Sequence[MigoDatabase]:
        return self.__migo_driver.get_databases()
