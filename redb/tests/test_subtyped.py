from typing import Literal, Optional

import pytest

from redb.behaviors import SubTypedDocument
from redb.interface.fields import ClassField


class SepecialCat(SubTypedDocument):
    name: str
    breed: str = "Siberian"
    created_by: str
    retired_by: Optional[str] = None

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [cls.name, cls.created_at]  # type: ignore


class WildCat(SepecialCat):
    type: Literal["WildCat"] = "WildCat"
    color: str


class DomesticCat(SepecialCat):
    type: Literal["DomesticCat"] = "DomesticCat"
    is_neutered: bool


class FeralCat(WildCat):
    is_violent: bool


@pytest.fixture(scope="session")
def creator_email():
    return "creator@cats.com"


@pytest.fixture(scope="function")
def cat(creator_email) -> SepecialCat:
    return SepecialCat(name="Fluffy", created_by=creator_email)


@pytest.fixture(scope="function")
def wild_cat(creator_email) -> WildCat:
    return WildCat(name="schsss", color="black", created_by=creator_email)


@pytest.fixture(scope="function")
def domestic_cat(creator_email) -> DomesticCat:
    return DomesticCat(name="schsss", is_neutered=True, created_by=creator_email)


@pytest.fixture(scope="function")
def feral_cat(creator_email) -> FeralCat:
    return FeralCat(
        name="zchhss", color="gray", is_violent=True, created_by=creator_email
    )


@pytest.fixture(scope="function", autouse=True)
def teardown():
    yield
    SepecialCat.delete_many({})


@pytest.fixture(scope="module")
def user_email():
    return "test_user@mail.com"


def test_subtyped_document_insert(
    cat: SepecialCat,
    wild_cat: WildCat,
    domestic_cat: DomesticCat,
    feral_cat: FeralCat,
):
    wild_cat.insert()
    domestic_cat.insert()

    wild_cat = SepecialCat.st_find_one({"name": wild_cat.name})
    domestic_cat = SepecialCat.st_find_one({"name": domestic_cat.name})

    assert cat.name == "Fluffy"
    assert wild_cat.name == "schsss"
    assert domestic_cat.name == "schsss"
    with pytest.raises(TypeError):
        cat.insert()
    with pytest.raises(TypeError):
        feral_cat.insert()
