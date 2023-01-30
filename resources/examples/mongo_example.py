from __future__ import annotations

import os
from typing import Optional

import dotenv

from redb.core import (
    BaseDocument,
    ClassField,
    CompoundIndex,
    Document,
    Index,
    MongoConfig,
    RedB,
)
from redb.interface.fields import Direction

dotenv.load_dotenv()


class API(BaseDocument):
    name: str


class Model(BaseDocument):
    name: str
    type: str
    provider: API


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    model: Model
    vector: list[float]
    other_models: Optional[list[Model]]
    source_url: str

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return [
            Index(field=cls.id),
            Index(field=cls.model, name="index_namerino"),  # cls.model should raise
            CompoundIndex(fields=[cls.text, cls.kb_name], unique=True),
            Index(field=cls.model.provider.name, direction=Direction.ASCENDING),
        ]

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        indices = [
            cls.kb_name,
            cls.model.provider.name,
            cls.other_models[0].provider.name,
        ]
        return indices


def main():
    config = MongoConfig(
        database_uri=os.environ["MONGODB_URI"], default_database="teia"
    )
    RedB.setup(config=config)
    Embedding.create_indexes()

    d = Embedding(
        kb_name="KB",
        model=Model(
            name="big-and-strong", type="encoder", provider=API(name="OpenTeia")
        ),
        text="Some data.VVW",
        vector=[1, 2, 0.1],
        source_url="www",
    )
    hashable_fields = Embedding.get_hashable_fields()
    for m_field in hashable_fields:
        print(m_field.resolve(d))
    print(d)
    from pymongo.errors import DuplicateKeyError

    try:
        print(d.insert_one())
    except DuplicateKeyError as e:
        print(dict(e.details))
    print(d.get_hash())


if __name__ == "__main__":
    main()
