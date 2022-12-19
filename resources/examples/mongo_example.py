from __future__ import annotations

from redb import Document, RedB
from redb.mongo_system import MongoConfig


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str


def main():
    config = MongoConfig(database_uri="mongodb://localhost:27017/teia")
    RedB.setup(config=config)
    client = RedB.get_client("mongo")
    db = client.get_default_database()
    [print(col.__collection_name__) for col in db.get_collections()]

    d = Embedding(
        kb_name="KB",
        model="big-and-strong",
        text="Some data.",
        vector=[1, 2, 0.1],
        source_url="www",
    )
    print(Embedding.delete_many(d))
    print(d.insert_one())
    print(Embedding.replace_one(filter=d, replacement=d, upsert=True))


if __name__ == "__main__":
    main()
