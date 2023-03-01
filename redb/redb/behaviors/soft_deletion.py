from ..core import Document
from ..interface.errors import DocumentNotFound
from ..core.document import IncludeColumns, OptionalDocumentData
from ..interface.fields import IncludeColumn
import functools

from typing import Type, TypeVar


T = TypeVar("T")


class SoftDeletinDoc(Document):
    is_deleted: bool = False

    @classmethod
    def soft_delete_one(cls, identifier):
        result = cls.update_one(dict(_id=identifier), dict(is_deleted=True))
        if not result.matched_count:
            raise DocumentNotFound

    @classmethod
    def soft_undelete_one(cls, identifier):
        result = cls.update_one(dict(_id=identifier), dict(is_deleted=False))
        if not result.matched_count:
            raise DocumentNotFound

    @classmethod
    def soft_delete_many(cls, filters):
        result = cls.update_many(filters, dict(is_deleted=True))
        if not result.matched_count:
            raise DocumentNotFound

    @classmethod
    def soft_undelete_many(cls, filters):
        result = cls.update_many(filters, dict(is_deleted=False))
        if not result.matched_count:
            raise DocumentNotFound

    @classmethod
    def find_one(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> T:
        if filter is None:
            filters = {}
        elif isinstance(filter, dict) and "is_deleted" not in filter:
            filter["is_deleted"] = False
        # if it were an instance of Document, it would either
        # already be using the default value
        # or the user would have set his own. Either way, it's ok.
        if fields is None:
            fields = [IncludeColumn(name="is_deleted", include=False)]
        # if it's a list of strings or a list of include columns, it's fine.
        return super().find_one(filter, fields, skip)