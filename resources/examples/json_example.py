from __future__ import annotations

from redb import Document, Field, FieldIndice, RedB
from redb.json_system import JSONCollection


class Embedding(Document):
    kb_name: str
    model: str = Field(index=FieldIndice(group_name="nop"))
    text: str = Field(
        index=FieldIndice(group_name="hu3", order=2, direction=1, unique=True)
    )
    vector: list[float]
    source_url: str = Field(
        index=FieldIndice(group_name="hu3", name="sourceUrl", order=1, direction=0)
    )


def main():
    config = dict(
        client_folder_path="resources",
        default_database_folder_path="db",
    )
    RedB.setup(backend="json", config=config)
    
    client = RedB.get_client("json")
    db = client.get_default_database()
    collections = db.get_collections()
    for col in collections:
        docs = JSONCollection.find(filter=col)
        for doc in docs:
            print(doc)

    d = Embedding(
        kb_name="KB",
        model="big-and-strong",
        text="Some data.",
        vector=[1, 2, 0.1],
        source_url="www",
    )
    print(Embedding.delete_many(filter=d))
    print(d.insert_one())
    print(Embedding.replace_one(filter=d, replacement=d, upsert=True))
    print(
        Embedding.insert_vectors(
            dict(
                kb_name=["a", "b"],
                model=["alpha", "beta"],
                text=["some data", "another data"],
                vector=[[1, 2], [3, 4]],
                source_url=["www", "com"],
            )
        )
    )


if __name__ == "__main__":
    main()
