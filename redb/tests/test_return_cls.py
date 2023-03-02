from redb.core.document import Document, _get_return_cls, _format_fields
from redb.interface.fields import IncludeColumn


class Scarecrow(Document):
    name: str
    other: None = None


# _format_fields([IncludeColumn(name="name", include=True), IncludeColumn(name="other", include=False)])
def test_return_cls():
    klass = _get_return_cls(Scarecrow, {"name": True, "other": False})
    assert klass is Scarecrow
    klass = _get_return_cls(Scarecrow, {"name": False, "other": False})
    assert klass is dict  # name is unselected
    klass = _get_return_cls(Scarecrow, {"name": False, "other": True})
    assert klass is dict  # name is unselected
    klass = _get_return_cls(Scarecrow, {"name": True, "other": True})
    assert klass is Scarecrow
    klass = _get_return_cls(Scarecrow, {"name": True})
    assert klass is Scarecrow
    klass = _get_return_cls(Scarecrow, {"other": False})
    assert klass is Scarecrow
    klass = _get_return_cls(Scarecrow, {"other": True})
    assert klass is dict  # missing name required
    klass = _get_return_cls(Scarecrow, {"name": False})
    assert klass is dict  # name is unselected
