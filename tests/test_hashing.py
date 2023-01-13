from redb import ClassField, CompoundIndex, Document, Field, Index, RedB
from redb.interfaces import Direction
from redb.mongo_system import MongoConfig


def test_simple_case():
    class Doc(Document):
        attr1: str
        attr2: str
        @classmethod
        def get_hashable_fields(cls) -> list[ClassField]:
            return [cls.attr1]

    obj = Doc(attr1="test")
    hashable_fields = obj.get_hashable_fields()
    key_val_tuples = obj._get_key_value_tuples_for_hash(hashable_fields)
    assert key_val_tuples == [('attr1', 'test')]


def test_
