import shutil
from pathlib import Path

from ..interfaces import Client
from .config import MilvusConfig


class MilvusClient(Client):
    def __init__(self, config: MilvusConfig) -> None:
        pass
    def _get_driver_client(self) -> Client:
        return self.__client

    def get_databases(self):
        pass

    def get_database(self, name: str):
        pass

    def get_default_database(self):
        return self.__default_database

    def drop_database(self, name: str) -> None:
        self.__client.drop_database(name)

    def close(self) -> None:
        self.__client.close()
