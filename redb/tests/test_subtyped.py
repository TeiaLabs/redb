from typing import Literal, Optional

import pytest

from redb.behaviors import SubTypedDocument
from redb.interface.fields import ClassField


class SpecialCat(SubTypedDocument):
    name: str
    breed: str = "Siberian"
    created_by: str
    retired_by: Optional[str] = None

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name, cls.created_at]  # type: ignore


class WildCat(SpecialCat):
    type: Literal["WildCat"] = "WildCat"
    color: str


class DomesticCat(SpecialCat):
    type: Literal["DomesticCat"] = "DomesticCat"
    is_neutered: bool


class FeralCat(WildCat):
    is_violent: bool


@pytest.fixture(scope="session")
def creator_email():
    return "creator@cats.com"


@pytest.fixture(scope="function")
def cat(creator_email) -> SpecialCat:
    return SpecialCat(name="Fluffy", created_by=creator_email)


@pytest.fixture(scope="function")
def wild_cat(creator_email) -> WildCat:
    return WildCat(name="schsss", color="black", created_by=creator_email)


@pytest.fixture(scope="function")
def domestic_cat(creator_email) -> DomesticCat:
    return DomesticCat(name="furrball", is_neutered=True, created_by=creator_email)


@pytest.fixture(scope="function")
def feral_cat(creator_email) -> FeralCat:
    return FeralCat(
        name="zchhss", color="gray", is_violent=True, created_by=creator_email
    )


@pytest.fixture(scope="function", autouse=True)
def teardown():
    yield
    SpecialCat.delete_many({})


@pytest.fixture(scope="module")
def user_email():
    return "test_user@mail.com"


def test_subtyped_document_insert(
    cat: SpecialCat,
    wild_cat: WildCat,
    domestic_cat: DomesticCat,
    feral_cat: FeralCat,
):
    wild_cat.insert()
    domestic_cat.insert()

    wild_cat = SpecialCat.st_find_one({"name": wild_cat.name})
    domestic_cat = SpecialCat.st_find_one({"name": domestic_cat.name})

    assert wild_cat.name == "schsss"
    assert domestic_cat.name == "furrball"
    with pytest.raises(TypeError):
        cat.insert()
    with pytest.raises(TypeError):
        feral_cat.insert()


def test_st_insert_one(
    wild_cat: WildCat,
    domestic_cat: DomesticCat,
):
    with pytest.raises(TypeError):
        SubTypedDocument.st_insert_one({})
    with pytest.raises(TypeError):
        FeralCat.st_insert_one({})

    WildCat.st_insert_one(wild_cat.dict())
    DomesticCat.st_insert_one(domestic_cat.dict())

    wild_cat = SpecialCat.st_find_one({"name": "schsss"})
    domestic_cat = SpecialCat.st_find_one({"name": "furrball"})
    assert wild_cat.name == "schsss"
    assert domestic_cat.name == "furrball"


def test_subtyped_document_find_one(
    wild_cat: WildCat,
    domestic_cat: DomesticCat,
):
    wild_cat.insert()
    domestic_cat.insert()

    wild_cat = SpecialCat.st_find_one({"name": wild_cat.name})
    domestic_cat = SpecialCat.st_find_one({"name": domestic_cat.name})

    assert wild_cat.name == "schsss"
    assert domestic_cat.name == "furrball"


def test_subtyped_document_find_many(
    wild_cat: WildCat,
    domestic_cat: DomesticCat,
):
    domestic_cat.insert()
    wild_cat.insert()

    cats = SpecialCat.st_find_many()
    assert len(cats) == 2
    assert isinstance(cats[0], DomesticCat)
    assert isinstance(cats[1], WildCat)

    cat = WildCat.st_find_many()
    assert len(cat) == 1
    assert cat[0].__class__ == WildCat

    cat = DomesticCat.st_find_many()
    assert len(cat) == 1
    assert cat[0].__class__ == DomesticCat
