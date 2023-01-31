from redb.core import BaseDocument, ClassField, Document, RedB
from redb.interface.configs import MongoConfig
from redb.interface.fields import CompoundIndex, Direction, Field, Index


def test_simple_case():
    class Cat(BaseDocument):
        name: str

    class Doc(Document):
        attr1: str
        attr2: str
        cat: Cat

        @classmethod
        def get_hashable_fields(cls) -> list[ClassField]:
            return [Doc.attr1, Doc.cat.name]

    obj = Doc(attr1="test", attr2="test2", cat=Cat(name="wisk"))
    hashable_fields = obj.get_hashable_fields()
    key_val_tuples = obj._get_key_value_tuples_for_hash(hashable_fields)
    assert key_val_tuples == [("attr1", "test"), ("cat.name", "wisk")]
