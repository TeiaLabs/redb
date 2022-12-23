from __future__ import annotations

from redb import Document, Indice, CompoundIndice, RedB
from redb.json_system import JSONCollection


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str

    @classmethod
    def get_indices(cls) -> list[Indice | CompoundIndice]:
        return [
            Indice(field=Embedding.model)
        ]


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
