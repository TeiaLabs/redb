import os

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from redb.core import Document, RedB, MongoConfig
from redb.interface.errors import UniqueConstraintViolation
from redb.interface.fields import CompoundIndex, Index


@pytest.fixture(scope="module", autouse=True)
def client():
    RedB.setup(
        MongoConfig(
            database_uri=os.environ["MONGODB_URI"],
        )
    )

@pytest.fixture(scope="module", autouse=True)
def db():
    return MongoClient(os.environ["MONGODB_URI"]).get_database()

class Cat(Document):
    name: str
    breed: str = "Domestic Shorthair"
    created_by: str

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
         return [CompoundIndex([cls.name, cls.breed], unique=True)]  # type: ignore

    @classmethod
    def collection_name(cls) -> str:
        return "cats"


def test_unique_constraints(db: Database):
    db["cats"].drop()
    Cat.create_indexes()
    cat = Cat(name="Kitty", created_by="me")
    result = Cat.insert_one(cat)
    assert result.inserted_id is not None
    assert cat == Cat.find_one({"_id": result.inserted_id})
    cat2 = cat = Cat(name="Kitty", created_by="other")
    with pytest.raises(UniqueConstraintViolation):
        Cat.insert_one(cat2)
    try:
        Cat.insert_one(cat2)
    except UniqueConstraintViolation as e:
        assert e.collection_name == "cats"
        assert e.dup_keys == [{"name": "Kitty", "breed": "Domestic Shorthair"}]
