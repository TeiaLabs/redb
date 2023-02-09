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
        response = Embedding.find_one({"_id": emb_id.inserted_id})
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
        vladmir_from_db = RussianDog.find_one({"_id": vladmir_id.inserted_id})
        vladmir_breed = RussianDog.find_one(vladmir, fields=["breed"])
        assert vladmir_breed == {
            "_id": vladmir_id.inserted_id,
            "breed": "Siberian Husky",
        }
        assert vladmir == vladmir_from_db

    def test_update_one(self):
        boris = RussianDog(
            name="Boris",
            age=2,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=True,
        )

        boris_id = RussianDog.insert_one(boris)
        RussianDog.update_one(filter=boris, update={"age": 3})
        boris.age = 3
        upsert_result = RussianDog.update_one(
            filter=boris, update={"is_good_boy": False}, upsert=True
        )
        boris_from_db = RussianDog.find_one({"_id": boris_id.inserted_id})

        assert upsert_result.matched_count == 1
        assert upsert_result.modified_count == 1
        assert boris_from_db.age == 3
        assert boris_from_db.is_good_boy == False

        irina = RussianDog(
            name="Irina",
            age=2,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=True,
        )
        upsert_result = RussianDog.update_one(
            filter=irina, update={"color": "White"}, upsert=True
        )
        irina.color = "White"
        irina_from_db = RussianDog.find_one({"_id": upsert_result.upserted_id})
        assert irina == irina_from_db
        assert upsert_result.matched_count == 0
        assert upsert_result.modified_count == 0

    def test_delete_one(self):
        svetlana = RussianDog(
            name="Svetlana",
            age=2,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=True,
        )
        svetlana_id = RussianDog.insert_one(svetlana)
        svetlana_from_db = RussianDog.find_one({"_id": svetlana_id.inserted_id})
        assert svetlana_from_db == svetlana

        RussianDog.delete_one(svetlana)
        with pytest.raises(TypeError):
            svetlana_from_db = RussianDog.find_one({"_id": svetlana_id.inserted_id})
