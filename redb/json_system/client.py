import shutil
from pathlib import Path

from ..interfaces import Client, Database
from .config import JSONConfig
from .database import JSONDatabase


class JSONClient(Client):
    def __init__(self, json_config: JSONConfig) -> None:
        self.__client_folder_path = Path(json_config.client_folder_path)

        database_default_path = json_config.default_database_folder_path
        if database_default_path is None:
            self.__default_database = JSONDatabase(
                next(self.__client_folder_path.glob("*"))
            )
        else:
            self.__default_database = JSONDatabase(
                self.__client_folder_path / database_default_path
            )

    def _get_driver_client(self) -> Client:
        return self.__client_folder_path

    def get_databases(self) -> list[Database]:
        return [
            JSONDatabase(folder)
            for folder in self.__client_folder_path.glob("*")
            if folder.is_dir()
        ]

    def get_database(self, name: str) -> Database:
        for folder in self.__client_folder_path.glob("*"):
            if folder.is_dir() and folder.name == name:
                return JSONDatabase(folder)

        raise ValueError(f"Database {name} not found")

    def get_default_database(self) -> Database:
        return self.__default_database

    def drop_database(self, name: str) -> None:
        shutil.rmtree(self.get_database(name).database_path)

    def close(self) -> None:
        return
