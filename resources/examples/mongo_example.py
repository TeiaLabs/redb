from __future__ import annotations

import os
from datetime import datetime

import dotenv
import pydantic

from redb import Document, Field, RedB, Index, CompoundIndex
from redb.mongo_system import MongoConfig
from redb.interfaces import Direction

dotenv.load_dotenv()


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


class Model(pydantic.BaseModel):
    name: str
    type: str
    provider: API


class API(pydantic.BaseModel):
    name: str


API.update_forward_refs()
Model.update_forward_refs()
Embedding.update_forward_refs()


def main():
    config = MongoConfig(database_uri=os.environ["MONGODB_URI"])
    RedB.setup(config=config)
    client = RedB.get_client("mongo")
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
        model="big-and-strong",
        text="Some data.",
        vector=[1, 2, 0.1],
        source_url="www",
    )
    print(d)
    print(Dog.delete_many(d))
    print(d.insert_one())
    print(Dog.replace_one(filter=d, replacement=d, upsert=True))


if __name__ == "__main__":
    main()
