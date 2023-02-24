from typing import Type

import pytest

from redb.core import BaseDocument, ClassField, Document


class Cat(BaseDocument):
    name: str


class Kitten(Document):
    is_bad_kitten: bool
    mom: Cat
    name: str

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name, cls.mom.name]


def test_key_val_tuples_construction():
    obj = Kitten(name="Fluffy", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    hashable_fields = obj.get_hashable_fields()
    key_val_tuples = obj._get_key_value_tuples_for_hash(hashable_fields)
    assert key_val_tuples == [("name", "Fluffy"), ("mom.name", "Whiskers")]


def test_hashable_fields_construction_order():
    class Doc(Document):
        attr1: str
        attr2: str
        attr3: str

        @classmethod
        def get_hashable_fields(cls) -> list[ClassField]:
            return [cls.attr1, cls.attr3]

    d = Doc(attr1="field1", attr2="field2", attr3="field3")
    hashable_fields = d.get_hashable_fields()
    key_val_tuples = d._get_key_value_tuples_for_hash(hashable_fields)
    assert key_val_tuples == [("attr1", "field1"), ("attr3", "field3")]

    class Doc(Document):
        attr1: str
        attr2: str
        attr3: str

        @classmethod
        def get_hashable_fields(cls) -> list[ClassField]:
            return [cls.attr2, cls.attr1]

    d = Doc(attr1="field1", attr2="field2", attr3="field3")
    hashable_fields = d.get_hashable_fields()
    key_val_tuples = d._get_key_value_tuples_for_hash(hashable_fields)
    assert key_val_tuples == [("attr2", "field2"), ("attr1", "field1")]


def test_equal_objects_equal_hash():
    obj1 = Kitten(name="Fluffy", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    obj1_hash = obj1.get_hash()
    obj2 = Kitten(name="Fluffy", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    obj2_hash = obj2.get_hash()
    assert obj1_hash == obj2_hash


def test_objects_only_different_by_non_hashable_fields():
    obj1 = Kitten(name="Fluffy", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    obj1_hash = obj1.get_hash()
    obj2 = Kitten(name="Fluffy", is_bad_kitten=True, mom=Cat(name="Whiskers"))
    obj2_hash = obj2.get_hash()
    assert obj1_hash == obj2_hash


def test_different_objects_different_hash():
    obj1 = Kitten(name="Fluffy", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    obj1_hash = obj1.get_hash()
    obj2 = Kitten(name="Oscar", is_bad_kitten=False, mom=Cat(name="Whiskers"))
    obj2_hash = obj2.get_hash()
    assert obj1_hash != obj2_hash
