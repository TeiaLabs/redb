import shutil
from pathlib import Path

from ..interfaces import (
    Client,
    Collection,
    Database,
)

from .collection import JSONCollection


class JSONDatabase(Database):
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    def get_collections(self) -> list[Collection]:
        return [
            JSONCollection(folder)
            for folder in self.database_path.glob("*")
            if folder.is_dir()
        ]

    def get_collection(self, name: str) -> Collection:
        for folder in self.database_path.glob("*"):
            if folder.is_dir() and folder.name == name:
                return JSONCollection()

        raise ValueError(f"Database {name} not found")

    def create_collection(self, name: str) -> None:
        (self.database_path / name).mkdir(exist_ok=True)

    def delete_collection(self, name: str) -> None:
        shutil.rmtree(self.get_collection(name).__class__.__name__)

    def __getitem__(self, name) -> Database:
        return JSONCollection()

    def get_client(self) -> Client:
        return self.client
