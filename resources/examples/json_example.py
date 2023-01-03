from __future__ import annotations

from pydantic import BaseModel

from redb import CompoundIndex, Document, Index, RedB
from redb.json_system import JSONCollection


class Dog(Document):
    name: str
    birthday: str
    other: SubclassMember

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return [Index(field=Dog.other.another.name)]


class SubclassMember(BaseModel):
    name: str
    another: SubSubClassMember


class SubSubClassMember(BaseModel):
    name: float


SubclassMember.update_forward_refs()
SubSubClassMember.update_forward_refs()
Dog.update_forward_refs()


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return [Index(field=Embedding.model)]


Embedding.update_forward_refs()


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
    print(d.model)
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
