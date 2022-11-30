import shutil
from pathlib import Path

from ..interfaces import (
    Client,
    Database,
)

from .database import JSONDatabase

class JSONClient(Client):
    def __init__(self, client_path: Path, database_path: Path | None = None) -> None:
        self.client_path = client_path
        if database_path is None:
            self.default_database = JSONDatabase(next(self.client_path.glob("*")))
        else:
            self.default_database = JSONDatabase(database_path)

    def get_databases(self) -> list[Database]:
        return [
            JSONDatabase(folder)
            for folder in self.client_path.glob("*")
            if folder.is_dir()
        ]

    def get_database(self, name: str) -> Database:
        for folder in self.client_path.glob("*"):
            if folder.is_dir() and folder.name == name:
                return JSONDatabase(folder)

        raise ValueError(f"Database {name} not found")

    def get_default_database(self) -> Database:
        return self.default_database

    def drop_database(self, name: str) -> None:
        shutil.rmtree(self.get_database(name).database_path)

    def close(self) -> None:
        return
