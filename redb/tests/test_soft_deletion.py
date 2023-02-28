import pytest
from redb.behaviors import SoftDeletinDoc
from redb.interface.errors import DocumentNotFound


class Cat(SoftDeletinDoc):
    name: str


def test_soft_deletion():
    obj = Cat(name="Fluffy")
    obj.insert()
    Cat.soft_delete_one(obj.id)
    with pytest.raises(DocumentNotFound):
        Cat.find_one(dict(_id=obj.id))
