import os
from typing import Optional

import pytest
from pymongo import MongoClient
from pymongo.database import Database
from pydantic import Field

from redb.behaviors import IRememberDoc
from redb.core import RedB
from redb.interface.configs import MongoConfig
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import ClassField


class Cat(IRememberDoc):
    name: str
    breed: str = "Domestic Shorthair"
    friends: list[str] = Field(default_factory=list)
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


def test_add_to_set(db: Database, creator_email: str, user_email: str, fluffy_cat: Cat):
    fluffy_cat.insert()
    up_result = Cat.historical_update_one(
        {"name": fluffy_cat.name},
        {"friends": "Pony"},
        operator="$addToSet",
        user_info=user_email,
    )
    assert up_result.modified_count == 1
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}))
    assert len(hist_cats) == 1
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 1
    assert hist_cat["created_by"] == creator_email
    assert hist_cat["retired_by"] == user_email
    assert len(hist_cat["friends"]) == 0
    cats = list(db["cats"].find({"name": fluffy_cat.name}))
    assert len(cats) == 1
    cat = cats[0]
    assert "Pony" in cat["friends"]



def test_add_each_to_set(db: Database, creator_email: str, user_email: str, fluffy_cat: Cat):
    fluffy_cat.insert()
    friends = ["Doggy", "Pony"]
    up_result = Cat.historical_update_one(
        {"name": fluffy_cat.name},
        {"friends": {"$each": friends}},
        operator="$addToSet",
        user_info=user_email,
    )
    assert up_result.modified_count == 1
    hist_cats = list(db["cats-history"].find({"name": fluffy_cat.name}))
    assert len(hist_cats) == 1
    hist_cat = hist_cats[0]
    assert hist_cat["version"] == 1
    assert hist_cat["created_by"] == creator_email
    assert hist_cat["retired_by"] == user_email
    assert len(hist_cat["friends"]) == 0
    cats = list(db["cats"].find({"name": fluffy_cat.name}))
    assert len(cats) == 1
    cat = cats[0]
    assert all(friend in cat["friends"] for friend in friends)

