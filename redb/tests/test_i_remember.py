import os
from operator import attrgetter

import pytest

from redb.behaviors import IRememberDoc
from redb.core import RedB
from redb.interface.configs import MongoConfig
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import ClassField


class Cat(IRememberDoc):
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


def test_retire_one():
    Cat.delete_many({})
    Cat.clear_history()

    obj = Cat(name="Fluffy")
    obj.insert()

    res = Cat.retire_one({"_id": obj.id})
    deleted_result, inserted_result = res
    assert deleted_result.deleted_count
    assert inserted_result.inserted_id


def test_find_history():
    Cat.delete_many({})
    Cat.clear_history()

    obj = Cat(name="Pony")
    obj.insert()

    del_res, insert_res = Cat.retire_one({"_id": obj.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id

    history = Cat.find_history(filter={"name": "Pony"})
    assert history.version == 1  # type: ignore
    assert history.name == obj.name  # type: ignore


def test_find_histories():
    Cat.delete_many({})
    Cat.clear_history()

    obj = Cat(name="My Little")
    obj.insert()

    del_res, insert_res = Cat.retire_one({"_id": obj.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id

    Cat.insert_one(obj.dict())

    del_res, insert_res = Cat.retire_one({"_id": obj.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id

    histories = Cat.find_histories()
    assert len(histories) == 2
