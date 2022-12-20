from __future__ import annotations

import os
from datetime import datetime

import dotenv

from redb import Document, Field, FieldIndice, RedB
from redb.mongo_system import MongoConfig

dotenv.load_dotenv()


class Dog(Document):
    name: str
    birthday: datetime
    # friends: list[Dog] = []  # TODO: make this work


class Embedding(Document):
    kb_name: str
    model: str = Field(index=FieldIndice(group_name="nop"))
    text: str = Field(index=FieldIndice(group_name="hu3"))
    vector: list[float]
    source_url: str = Field(index=FieldIndice(group_name="hu3", name="sourceUrl"))


def main():
    config = MongoConfig(database_uri=os.environ["MONGODB_URI"])
    RedB.setup(config=config)
    client = RedB.get_client("mongo")
    db = client.get_default_database()
    print([col for col in db.get_collections()])
    
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
