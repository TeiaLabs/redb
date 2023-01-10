from __future__ import annotations

import os
from datetime import datetime

import pydantic

from redb import CompoundIndex, Document, Field, Index, RedB
from redb.interfaces import Direction
from redb.mongo_system import MongoConfig


class Dog(Document):
    name: str
    birthday: datetime
    # friends: list[Dog] = []  # TODO: make this work


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    model: Model
    vector: list[float]
    source_url: str

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return [
            # Index(field=cls.model, name="index_name"),
            # CompoundIndex(fields=[cls.text, cls.kb_name], unique=True),
            Index(field=cls.model.provider.name, direction=Direction.ASCENDING),
        ]

    @property
    def hashable_fields(self):
        return ["kb_name", self.model.hashble_fields, "text"]


class Model(pydantic.BaseModel):
    name: str
    type: str
    provider: API


class API(pydantic.BaseModel):
    name: str


def main():
    config = MongoConfig(database_uri="mongodb://localhost:27017/", default_database="teia")
    RedB.setup(config=config)
    client = RedB.get_client()
    Embedding.create_indices()

    db = client.get_default_database()
    for col in db.get_collections():
        [print(obj) for obj in col.find()]

    d = Dog(
        name="Spike",
        birthday=datetime.today(),
        # friends=[Dog(name="Lily", birthday=datetime.today())],
    )

    d = Embedding(
        kb_name="KB",
        model=Model(
            name="big-and-strong", type="encoder", provider=API(name="OpenTeia")
        ),
        text="Some data.",
        vector=[1, 2, 0.1],
        source_url="www",
    )
    print(d)
    print(Dog.delete_many(d))
    print(d.insert_one())
    print(Dog.replace_one(filter=d, replacement=d, upsert=True))
    print(d.get_hash())


if __name__ == "__main__":
    main()
