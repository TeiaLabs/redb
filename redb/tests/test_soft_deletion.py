import os
from operator import attrgetter

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
        assert cat.dict(exclude={"updated_at"}) == obj.dict(exclude={"updated_at"})
    finally:
        Cat.delete_one(filters)


def test_soft_delete_then_undelete_many():
    objs = [Cat(name="Fluffy"), Cat(name="Whiskers")]
    filters = {"_id": {"$in": [o.id for o in objs]}}
    try:
        Cat.insert_many(objs)
        Cat.soft_delete_many(filters)
        found_objs = Cat.find_many(filters)
        assert len(found_objs) == 0
        
        Cat.soft_undelete_many(filters)
        found_objs = Cat.find_many(filters)
        found_objs = sorted(found_objs, key=attrgetter("name"))
        assert len(objs) == len(found_objs)
        for expec, found in zip(objs, found_objs):
            assert expec.dict(exclude={"updated_at"}) == found.dict(exclude={"updated_at"})

    finally:
        Cat.delete_many(filters)
