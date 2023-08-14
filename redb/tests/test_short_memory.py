import os
import time
from typing import Optional

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from redb.behaviors import CachedDocument
from redb.core import RedB
from redb.interface.configs import MongoConfig
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import ClassField


class Cat(CachedDocument):
    name: str
    breed: str = "Domestic Shorthair"
    created_by: str
    retired_by: Optional[str] = None

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name]  # type: ignore

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


@pytest.fixture(scope="module")
def user_email():
    return "test_user@mail.com"


@pytest.mark.order(after="test_historical_delete_one")
def test_find_one(user_email, pony_cat: Cat):
    pony_cat.insert()

    cached_start = time.time()
    Cat.find_one(filter={"_id": pony_cat.id})
    cached_end = time.time()
    cached_duration = cached_end - cached_start
    
    uncached_start = time.time()
    Cat.find_one(filter={"_id": pony_cat.id}, force=True)
    uncached_end = time.time()
    uncached_duration = uncached_end - uncached_start

    assert cached_duration < uncached_duration
