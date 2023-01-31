from __future__ import annotations

from datetime import datetime

from redb.core import (
    BaseDocument,
    ClassField,
    CompoundIndex,
    Document,
    Field,
    Index,
    RedB,
)


class Dog(Document):
    name: str
    birthday: str
    other: SubclassMember

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return [Index(field=Dog.other.another.name)]


class SubclassMember(BaseDocument):
    name: str
    another: SubSubClassMember


class SubSubClassMember(BaseDocument):
    name: float


class Embedding(Document):
    class Config:
        json_encoders = {float: lambda _: ""}

    kb_name: str
    model: str
    text: str
    a: dict[str, list[dict[float, list[datetime]]]]
    vector: list[float] = Field(vector_type="FLOAT", dimensions=1)
    source_url: str

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.kb_name, cls.model]

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return [Index(field=Embedding.model)]


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
        a={"test": [{10: [datetime.utcnow()]}]},
    )
    print(d.dict())
    print(d.model)
    try:
        print(Embedding.delete_many(filter=d))
    except ValueError as e:
        print(e)

    try:
        print(d.insert_one())
        print(Embedding.replace_one(filter=d, replacement=d, upsert=True))
    except ValueError as e:
        print(e)

    print(
        Embedding.insert_vectors(
            dict(
                id=["a", "b"],
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
