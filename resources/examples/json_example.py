from __future__ import annotations

from redb import RedB
from redb.json_system import JSONCollection


class Embedding(JSONCollection):
    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str


def main():
    config = dict(
        client_folder_path="resources",
        default_database_folder_path="db",
    )
    RedB.setup(backend="json", config=config)

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
