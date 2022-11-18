import json
from typing import TypeVar, Type
import pandas as pd

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
            cls(**json.load(open(json_file)))
            for json_file in json_files
            if json_file.is_file()
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

    @staticmethod
    def insert_many(cls, data: list[Document] | list[dict]):
        for item in data:
            if isinstance(item, dict):
                item = cls(**item)

            if isinstance(item, cls):
                item.insert()
            else:
                raise ValueError(f"I don't know how to convert item {item} into {cls}")

    @staticmethod
    def insert_vectors(cls, data: pd.DataFrame):
        data = [cls(**x) for x in data.to_dict(orient="records")]
        return cls.insert_many(data)

    @staticmethod
    def find_by_id(cls, id):
        c = init_db.REDB.get_client()
        collection_path = c.attrs["dir_path"] / cls.collection_name()
        json_path = collection_path / Path(f"{id}.json")
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            return cls(**data)
        else:
            return None
