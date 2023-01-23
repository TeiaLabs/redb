from __future__ import annotations

import dotenv

from redb import Document, Field, MigoConfig, RedB
from redb.base import BaseDocument

dotenv.load_dotenv()


class Embedding(BaseDocument):
    """Semantic vector."""

    model_type: str
    model_name: str
    vector: list[float] = Field(vector_type="FLOAT", dimensions=1)


class Instance(Document):
    kb_name: str
    text: str
    embs: list[Embedding]
    source_url: str


def main():
    config = MigoConfig(
        milvus_connection_alias="",
        milvus_host="localhost",
        milvus_port=8000,
        mongo_database_uri=os.environ["MONGODB_URI"],
    )
    RedB.setup("milvus", config, globals())
    a = Embedding(
        model_type="a",
        model_name="b",
        vector=[1, 2, 3],
    )
    b = Instance(
        kb_name="KB",
        text="Some data.",
        embs=[a],
        source_url="www",
    )
    print(Embedding.delete_many(b))
    print(b.insert_one())
    print(Embedding.replace_one(filter=b, replacement=b, upsert=True))
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
