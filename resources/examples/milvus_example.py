from __future__ import annotations

from pathlib import Path

from redb import RedB
from redb.milvus_system import MilvusCollection, MilvusConfig


class Embedding(MilvusCollection):
    """Semantic vector."""
    model_type: str
    model_name: str
    vector: list[float]


class Instance(MilvusCollection):
    kb_name: str
    text: str
    embs: list[Embedding]
    source_url: str


def main():
    config = MilvusConfig(
        client_folder_path="resources",
    )
    RedB.setup("milvus", config, globals())
    a = Embedding(
        model_type="a",
        model_name="b",
        vector=[1,2,3],
    )
    b = Instance(
        kb_name="KB",
        text="Some data.",
        embs=[a],
        source_url="www",
    )
    print(a, b)
    exit()
    print(Embedding.delete_many(d))
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
