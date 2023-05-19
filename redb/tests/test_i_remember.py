import os

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


@pytest.fixture(scope="function")
def fluffy_cat() -> Cat:
    return Cat(name="Fluffy")


@pytest.fixture(scope="function")
def pony_cat() -> Cat:
    return Cat(name="Pony")


@pytest.fixture(scope="function")
def little_cat() -> Cat:
    return Cat(name="Little")


@pytest.fixture(scope="function", autouse=True)
def teardown(fluffy_cat: Cat, pony_cat: Cat, little_cat: Cat):
    yield
    cat_names = [fluffy_cat.name, pony_cat.name, little_cat.name]
    filters = {"name": {"$in": cat_names}}
    Cat.delete_many(filters)
    Cat.historical_delete_many(filters)


def test_delete_one(fluffy_cat: Cat):
    obj = fluffy_cat
    obj.insert()
    res = Cat.historical_delete_one({"_id": obj.id}, user_info="test_user@mail.com")
    deleted_result, inserted_result = res
    assert deleted_result.deleted_count
    assert inserted_result.inserted_id


@pytest.mark.order(after="test_delete_one")
def test_find_snapshot(pony_cat: Cat):
    obj = pony_cat
    obj.insert()
    del_res, insert_res = Cat.historical_delete_one({"_id": obj.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id
    history = Cat.find_snapshot(filter={"name": pony_cat.name})
    assert history.version == 1  # type: ignore
    assert history.name == obj.name  # type: ignore


@pytest.mark.order(after="test_delete_one")
def test_find_revisions(little_cat: Cat):
    Cat.insert_one(little_cat.dict())
    del_res, insert_res = Cat.historical_delete_one({"_id": little_cat.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id
    Cat.insert_one(little_cat.dict())
    del_res, insert_res = Cat.historical_delete_one({"_id": little_cat.id})
    assert del_res.deleted_count
    assert insert_res.inserted_id
    histories = Cat.find_revisions({"name": little_cat.name})
    # test default sort order descending on version
    assert len(histories) == 2
    assert histories[0].version == 2
    assert histories[1].version == 1
    latest = histories[0]
    previous = histories[1]
    assert latest.name == previous.name
    assert latest.created_at >= previous.created_at
    assert latest.retired_at > previous.retired_at
