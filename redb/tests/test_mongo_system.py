import os
from pathlib import Path

import dotenv
import pytest
from redb.interface.errors import DocumentNotFound
from redb.interface.fields import Direction, SortColumn

from .utils import Embedding, RussianDog

dotenv.load_dotenv()


class TestmongoSystem:

    @pytest.fixture(scope="class", autouse=True)
    def clean_db(self):
        RussianDog.delete_many({})
        Embedding.delete_many({})
        yield
        RussianDog.delete_many({})
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
            filter=boris.dict(exclude={"updated_at", "created_at"}),
            update={"is_good_boy": False},
            upsert=True,
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
        with pytest.raises(DocumentNotFound):
            svetlana_from_db = RussianDog.find_one({"_id": svetlana_id.inserted_id})

    def test_insert_many(self):
        dog = RussianDog(
            name="dog", age=5, breed="Siberian Husky", color="Gray", is_good_boy=True
        )
        dogdog = RussianDog(
            name="dogdog",
            age=6,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=False,
        )
        ids = RussianDog.insert_many([dog, dogdog])
        print(ids.inserted_ids)
        dogs = RussianDog.find_many({"_id": {"$in": ids.inserted_ids}})
        dogs = sorted(dogs, key=lambda x: x.name)
        assert dogs == [dog, dogdog]
        RussianDog.delete_many({"_id": {"$in": ids.inserted_ids}})
        dict_ids = RussianDog.insert_many([dog.dict(), dogdog.dict()])
        dict_dogs = RussianDog.find_many({"_id": {"$in": dict_ids.inserted_ids}})
        dict_dogs = sorted(dict_dogs, key=lambda x: x.name)
        assert dict_dogs == [dog.dict(), dogdog.dict()]

    def test_find_many(self):
        natasha = RussianDog(
            name="Natasha",
            age=5,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=True,
        )
        oksana = RussianDog(
            name="Oksana",
            age=6,
            breed="Siberian Husky",
            color="Gray",
            is_good_boy=False,
        )
        ids = RussianDog.insert_many([natasha, oksana])
        dogs = RussianDog.find_many({"_id": {"$in": ids.inserted_ids}})
        dogs = sorted(dogs, key=lambda x: x.name)
        assert dogs == [natasha, oksana]
        # fields
        dog_ages = RussianDog.find_many(
            filter={"_id": {"$in": ids.inserted_ids}}, fields=["age"]
        )
        dog_ages = sorted(dog_ages, key=lambda x: x["age"])
        assert dog_ages[0]["age"] == 5 and dog_ages[1]["age"] == 6
        # sorted
        sorted_dogs = RussianDog.find_many(
            filter={"_id": {"$in": ids.inserted_ids}},
            sort=SortColumn(name="name", direction=Direction.ASCENDING),
        )
        assert sorted_dogs == [natasha, oksana]

    def test_update_many(self):
        oleg = RussianDog(
            name="Oleg",
            age=10,
            breed="Siberian Husky",
            color="Blue",
            is_good_boy=True,
        )
        katerina = RussianDog(
            name="Katerina",
            age=5,
            breed="Siberian Husky",
            color="Blue",
            is_good_boy=True,
        )
        # upadate
        ids = RussianDog.insert_many([oleg, katerina])
        update_result = RussianDog.update_many(
            filter={"color": "Blue"}, update={"color": "Red"}
        )
        katerina.color = "Red"
        oleg.color = "Red"
        dogs = RussianDog.find_many({"_id": {"$in": ids.inserted_ids}})
        dogs = sorted(dogs, key=lambda x: x.age)
        assert update_result.matched_count == 2
        assert update_result.modified_count == 2
        assert dogs == [katerina, oleg]

        # upsert
        romeo = RussianDog(
            name="Romeo",
            age=7,
            breed="Dachshund",
            color="Brown",
            is_good_boy=True,
        )
        upsert_result = RussianDog.update_many(
            filter=romeo,
            update={"color": "Black"},
            upsert=True,
        )
        romeo.color = "Black"

        upseted_dog = RussianDog.find_one(filter={"_id": upsert_result.upserted_id})
        assert upsert_result.matched_count == 0
        assert upsert_result.modified_count == 0
        assert upseted_dog == romeo

        # allow new fields
        update_allow_result = RussianDog.update_many(
            filter={"color": "Red"}, update={"is_pet": True}, allow_new_fields=True
        )
        allow_dogs = RussianDog.find_many({"_id": {"$in": ids.inserted_ids}})
        allow_dogs = sorted(allow_dogs, key=lambda x: x.age)
        katerina.is_pet = True
        oleg.is_pet = True

        assert update_allow_result.matched_count == 2
        assert update_allow_result.modified_count == 2
        assert allow_dogs[0] == katerina
        assert allow_dogs[1] == oleg

    def test_delete_many(self):
        viktor = RussianDog(
            name="Viktor",
            age=13,
            breed="Siberian Husky",
            color="White",
            is_good_boy=True,
        )
        alek = RussianDog(
            name="Alek",
            age=13,
            breed="Siberian Husky",
            color="White",
            is_good_boy=True,
        )
        ids = RussianDog.insert_many([viktor, alek])
        dogs = RussianDog.find_many({"_id": {"$in": ids.inserted_ids}})
        dogs = sorted(dogs, key=lambda x: x.name)
        assert [alek, viktor] == dogs

        RussianDog.delete_many(filter={"age": 13})
        with pytest.raises(DocumentNotFound):
            RussianDog.find_one({"_id": ids.inserted_ids[0]})
        with pytest.raises(DocumentNotFound):
            RussianDog.find_one({"_id": ids.inserted_ids[1]})
