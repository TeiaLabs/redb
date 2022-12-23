from __future__ import annotations

import os
from datetime import datetime

import dotenv

from redb import Document, Field, RedB, Indice, CompoundIndice
from redb.mongo_system import MongoConfig

dotenv.load_dotenv()


class Dog(Document):
    name: str
    birthday: datetime
    # friends: list[Dog] = []  # TODO: make this work


class Embedding(Document):
    kb_name: str
    model: str
    text: str 
    vector: list[float]
    source_url: str

    @classmethod
    def get_indices(cls) -> list[Indice | CompoundIndice]:
        return [
            Indice(field=Embedding.model, name="index_name"),
            CompoundIndice(fields=[Embedding.text, Embedding.kb_name], unique=True)
        ]


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
        friends=[Dog(name="Lily", birthday=datetime.today())],
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
