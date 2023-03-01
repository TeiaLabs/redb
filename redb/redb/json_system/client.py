import shutil
from pathlib import Path

from redb.interface.client import Client
from redb.interface.configs import JSONConfig

from .database import JSONDatabase


class JSONClient(Client):
    def __init__(self, json_config: JSONConfig | dict) -> None:
        if isinstance(json_config, dict):
            json_config = JSONConfig(**json_config)
        self.__client_folder_path = Path(json_config.client_folder_path)

        database_default_path = json_config.default_database_folder_path
        if database_default_path is None:
            self.__default_database = JSONDatabase(
                self.__client_folder_path / Path("default-database")
            )
        else:
            self.__default_database = JSONDatabase(
                self.__client_folder_path / database_default_path
            )

    def _get_driver_client(self) -> "JSONClient":
        return self

    def get_databases(self) -> list[JSONDatabase]:
        return [
            JSONDatabase(folder)
            for folder in self.__client_folder_path.glob("*")
            if folder.is_dir()
        ]

    def get_database(self, name: str) -> JSONDatabase:
        for folder in self.__client_folder_path.glob("*"):
            if folder.is_dir() and folder.name == name:
                return JSONDatabase(folder)

        raise ValueError(f"Database {name} not found")

    def get_default_database(self) -> JSONDatabase:
        return self.__default_database

    def drop_database(self, name: str) -> bool:
        try:
            shutil.rmtree(self.get_database(name).database_path)
            return True
        except:
            return False

    def close(self) -> bool:
        return True

    def __truediv__(self, other):
        return self.__client_folder_path / other
