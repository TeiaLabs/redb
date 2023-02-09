from pathlib import Path

import pytest

from redb.core import RedB
from redb.interface.configs import MongoConfig
from .utils import Embedding, RussianDog

class TestmongoSystem:
    @pytest.fixture()
    def client_path(self):
        return Path("/tmp/")

    @pytest.fixture(scope="class", autouse=True)
    def client(self):
        RedB.setup(
            MongoConfig(
                database_uri="mongodb://localhost:27017/redb-tests",
            )
        )

    @pytest.fixture(scope="class", autouse=True)
    def clean_db(self, client):
        Embedding.delete_many({})

    def test_insert_one(self):
        emb = Embedding(
            kb_name="KB",
            model="ai",
            text="Some data.",
            vector=[1, 2],
            source_url="www",
        )
        emb_id = Embedding.insert_one(emb)
        response = Embedding.find_one({"_id":emb_id.inserted_id})
        assert emb == response

    def test_find_one(self):
        vladmir = RussianDog(
            name="Vladimir",
            age=2,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=True,
        )
        vladmir_id = RussianDog.insert_one(vladmir)
        vladmir_from_db = RussianDog.find_one({"_id":vladmir_id.inserted_id})
        vladmir_breed = RussianDog.find_one(vladmir, fields=["breed"])
        assert vladmir_breed == {"_id":vladmir_id.inserted_id,"breed": "Siberian Husky"}
        assert vladmir == vladmir_from_db

    def test_update_one(self):

        pass
    def test_delete_one(self):
        pass
