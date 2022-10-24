import json
from typing import TypeVar, Type

from ..document import Document
from .. import init_db

T = TypeVar("T")


class JSONDocument(Document):

    @classmethod
    def find_many(
        cls: Type[T],
        filters: dict | None = None,
        pagination: slice | None = None,
        projection: dict | None = None,
        sorting: dict | None = None,
    ) -> list[T]:
        """Ignore params and return all items."""
        c = init_db.REDB.get_client()
        json_path = c.attrs["dir_path"] / f"{cls.collection_name()}.json"
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            return [cls(**obj) for obj in data]
        return []

    def insert(self):
        c = init_db.REDB.get_client()
        json_path = c.attrs["dir_path"] / f"{self.collection_name()}.json"
        obj = self.dict()
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            data.append(obj)
        else:
            data = [obj]
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)
