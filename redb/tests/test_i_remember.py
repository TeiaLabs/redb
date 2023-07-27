import os
from typing import Optional

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from redb.behaviors import IRememberDoc
from redb.core import RedB
from redb.interface.configs import MongoConfig
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import ClassField


class Cat(IRememberDoc):
    name: str
    breed: str = "Domestic Shorthair"
    created_by: str
    retired_by: Optional[str] = None

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name, cls.breed]  # type: ignore

    @classmethod
    def collection_name(cls) -> str:
        return "cats"


@pytest.fixture(scope="module", autouse=True)
def client():
    RedB.setup(
        MongoConfig(
            database_uri=os.environ["MONGODB_URI"],
        )
    )


@pytest.fixture(scope="module")
def db() -> Database:
    return MongoClient(os.environ["MONGODB_URI"]).get_default_database()


@pytest.fixture(scope="session")
def creator_email():
    return "creator@cats.com"


@pytest.fixture(scope="function")
def fluffy_cat(creator_email) -> Cat:
    return Cat(name="Fluffy", created_by=creator_email)


@pytest.fixture(scope="function")
def pony_cat(creator_email) -> Cat:
    return Cat(name="Pony", created_by=creator_email)


@pytest.fixture(scope="function")
def little_cat(creator_email) -> Cat:
    return Cat(name="Little", created_by=creator_email)


@pytest.fixture(scope="function", autouse=True)
def teardown(fluffy_cat: Cat, pony_cat: Cat, little_cat: Cat):
    yield
    cat_names = [fluffy_cat.name, pony_cat.name, little_cat.name]
    filters = {"name": {"$in": cat_names}}
    Cat.delete_many(filters)
    Cat.delete_history(filters)


@pytest.fixture(scope="module")
def user_email():
    return "test_user@mail.com"


def test_historical_delete_one(db: Database, fluffy_cat: Cat, creator_email: str, user_email: str):
    obj = fluffy_cat
    obj.insert()
    deleted_result = Cat.historical_delete_one({"_id": obj.id}, user_info=user_email)
    assert deleted_result.deleted_count
    cats = list(db["cats"].find({"name": fluffy_cat.name}))
    assert len(cats) == 0
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}))
    assert len(hist_cats) == 1
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 1
    assert hist_cat["created_by"] == creator_email
    assert hist_cat["retired_by"] == user_email


def test_historical_version_incrementing(db: Database, fluffy_cat: Cat):
    obj = fluffy_cat
    obj.insert()
    Cat.historical_delete_one({"_id": obj.id}, user_info="user1@email.com")
    Cat(**obj.dict(exclude={"created_at"})).insert()
    Cat.historical_delete_one({"_id": obj.id}, user_info="user2@email.com")
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}, sort=[("version", -1)]))
    assert len(hist_cats) == 2
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 2
    assert hist_cat["retired_by"] == "user2@email.com"
    Cat(**obj.dict(exclude={"created_at"})).insert()
    Cat.historical_delete_one({"_id": obj.id}, user_info="user3@email.com")
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}, sort=[("version", -1)]))
    assert len(hist_cats) == 3
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 3
    assert hist_cat["retired_by"] == "user3@email.com"


@pytest.mark.order(after="test_historical_delete_one")
def test_historical_find_one(user_email, pony_cat: Cat):
    pony_cat.insert()
    del_res = Cat.historical_delete_one({"_id": pony_cat.id}, user_info=user_email)
    assert del_res.deleted_count
    history = Cat.historical_find_one(filter={"name": pony_cat.name})
    assert history.version == 1
    assert history.name == pony_cat.name


@pytest.mark.order(after="test_historical_delete_one")
def test_historical_find_many(little_cat: Cat):
    little_cat_obj = little_cat.dict(exclude={"created_at"})
    Cat(**little_cat_obj).insert()

    del_res = Cat.historical_delete_one({"_id": little_cat.id})
    assert del_res.deleted_count

    Cat(**little_cat_obj).insert()

    del_res = Cat.historical_delete_one({"_id": little_cat.id})
    assert del_res.deleted_count
    histories = Cat.historical_find_many({"name": little_cat.name})
    assert len(histories) == 2
    assert histories[0].version == 2
    assert histories[1].version == 1
    latest = histories[0]
    previous = histories[1]
    assert latest.name == previous.name
    assert latest.created_at >= previous.created_at
    assert ((l := latest.retired_at) is not None) and ((p := previous.retired_at) is not None) and l > p


def test_historical_update_one(db: Database, creator_email: str, user_email: str, fluffy_cat: Cat):
    fluffy_cat.insert()
    updated_result = Cat.historical_update_one(
        {"name": fluffy_cat.name}, {"breed": "American Bobtail"}, user_info=user_email
    )
    assert updated_result.modified_count
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}))
    assert len(hist_cats) == 1
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 1
    assert hist_cat["created_by"] == creator_email
    assert hist_cat["retired_by"] == user_email
    assert hist_cat["breed"] == fluffy_cat.breed
    cats = list(db["cats"].find({"name": fluffy_cat.name}))
    assert len(cats) == 1
    cat = cats[0]
    assert cat["breed"] == "American Bobtail"
    assert cat["_id"] != fluffy_cat.id


def test_historical_replace_one(db: Database, creator_email: str, user_email: str, fluffy_cat: Cat):
    fluffy_cat.insert()
    old_cat_id = fluffy_cat.id
    replaced_result = Cat.historical_replace_one(
        {"name": fluffy_cat.name}, {**fluffy_cat.dict(), "breed": "American Bobtail"}, user_info=user_email
    )
    assert replaced_result.modified_count
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}))
    assert len(hist_cats) == 1
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 1
    assert hist_cat["ref_id"] == old_cat_id
    assert hist_cat["created_by"] == creator_email
    assert hist_cat["retired_by"] == user_email
    assert hist_cat["breed"] == fluffy_cat.breed
    cats = list(db["cats"].find({"name": fluffy_cat.name}))
    assert len(cats) == 1
    cat = cats[0]
    assert cat["breed"] == "American Bobtail"
    assert cat["_id"] != fluffy_cat.id
