import shutil
from pathlib import Path

from ..interfaces import Database
from .collection import JSONCollection


class JSONDatabase(Database):
    def __init__(self, database_path: Path) -> None:
        self.__database_folder_path = database_path

    def _get_driver_database(self) -> "JSONDatabase":
        return self

    def get_collections(self) -> list[JSONCollection]:
        return [
            JSONCollection(folder)
            for folder in self.__database_folder_path.glob("*")
            if folder.is_dir()
        ]

    def get_collection(self, name: str) -> JSONCollection:
        for folder in self.__database_folder_path.glob("*"):
            if folder.is_dir() and folder.name == name:
                return JSONCollection(collection_name=name)

        raise ValueError(f"Database {name} not found")

    def create_collection(self, name: str) -> None:
        (self.__database_folder_path / name).mkdir(exist_ok=True)

    def delete_collection(self, name: str) -> None:
        shutil.rmtree(self.get_collection(name).__class__.__name__)

    def __getitem__(self, name) -> "JSONDatabase":
        return JSONCollection(collection_name=name)

    def __truediv__(self, other):
        return self.__database_folder_path / other
