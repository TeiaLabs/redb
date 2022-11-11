import json
from typing import TypeVar, Type

from ..document import Document
from .. import init_db
from pathlib import Path

T = TypeVar("T", bound=Document)


class JSONDocument(Document):

    @staticmethod
    def find_many(
        cls: Type[T],
        filters: dict | None = None,
        pagination: slice | None = None,
        projection: dict | None = None,
        sorting: dict | None = None,
    ) -> list[T]:
        """Ignore params and return all items."""
        c = init_db.REDB.get_client()
        collection_path = c.attrs["dir_path"] / cls.collection_name()
        json_files = collection_path.rglob("*json")

        return [
            cls(**json.load(open(_file)))
            for _file in json_files
            if _file.is_file()
        ]

    def insert(self):
        c = init_db.REDB.get_client()
        collection_path = c.attrs["dir_path"] / self.collection_name()
        collection_path.mkdir(parents=True, exist_ok=True)
        json_path = collection_path / Path(f"{self.get_hash()}.json")
        obj = self.dict()

        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            data.update(obj)
        else:
            data = obj
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)
