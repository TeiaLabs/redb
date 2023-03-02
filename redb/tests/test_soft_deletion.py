import os

import pytest
from redb.behaviors import SoftDeletinDoc
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import ClassField
from redb.core import RedB
from redb.interface.configs import MongoConfig

class Cat(SoftDeletinDoc):
    name: str

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name]  # type: ignore


@pytest.fixture(scope="module", autouse=True)
def client():
    RedB.setup(
        MongoConfig(
            database_uri=os.environ["MONGODB_URI"],
        )
    )


def test_soft_deletion():
    obj = Cat(name="Fluffy")
    filters = dict(_id=obj.id)
    try:
        obj.insert()
        Cat.soft_delete_one(filters)
        with pytest.raises(DocumentNotFound):
            Cat.find_one(filters)
    finally:
        Cat.delete_one(filters)


def test_soft_undeletion():
    obj = Cat(name="Fluffy")
    filters = dict(_id=obj.id)
    try:
        obj.insert()
        Cat.soft_delete_one(filters)
        with pytest.raises(DocumentNotFound):
            Cat.find_one(filters)
        Cat.soft_undelete_one(filters)
        cat = Cat.find_one(filters)
        assert cat == obj
    finally:
        Cat.delete_one(filters)

