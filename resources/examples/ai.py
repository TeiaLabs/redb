from __future__ import annotations

from pathlib import Path

from src.redb import JSONCollection, RedB


class Embedding(JSONCollection):
    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str


def main():
    client_dir = Path(".")
    db_dir = client_dir / "resources"
    RedB.setup("json", client_path=client_dir, database_path=db_dir)

    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    print(Embedding.delete_many(d))
    print(Embedding.replace_one(d, replacement=d, upsert=True))
    print(
        Embedding.insert_vectors(
            dict(
                kb_name=["a", "b"],
                model=["one", "two"],
                text=["some data", "another data"],
                vector=[[1, 2], [3, 4]],
                source_url=["www", "com"],
            )
        )
    )


if __name__ == "__main__":
    main()
