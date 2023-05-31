from typing import Type, TypeVar

from ..core import Document
from ..core.document import IncludeColumns, OptionalDocumentData, SortColumns
from ..interface.errors import DocumentNotFound
from ..interface.fields import ClassField, IncludeColumn

T = TypeVar("T")


class SoftDeletinDoc(Document):
    is_deleted: bool = False

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        all_fields = super().get_hashable_fields()
        return list(filter(lambda x: x.model_field.name != "is_deleted", all_fields))

    @classmethod
    def soft_delete_one(cls, filters):
        result = cls.update_one(filters, dict(is_deleted=True))
        if not result.matched_count:
            raise DocumentNotFound(collection_name=cls.collection_name())

    @classmethod
    def soft_undelete_one(cls, filters):
        result = cls.update_one(filters, dict(is_deleted=False))
        if not result.matched_count:
            raise DocumentNotFound(collection_name=cls.collection_name())

    @classmethod
    def soft_delete_many(cls, filters):
        result = cls.update_many(filters, dict(is_deleted=True))
        if not result.matched_count:
            raise DocumentNotFound(collection_name=cls.collection_name())

    @classmethod
    def soft_undelete_many(cls, filters):
        result = cls.update_many(filters, dict(is_deleted=False))
        if not result.matched_count:
            raise DocumentNotFound(collection_name=cls.collection_name())

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
            filters = filter | {"is_deleted": False}
        else:
            filters = filter
        # if it were an instance of Document, it would either
        # already be using the default value
        # or the user would have set his own. Either way, it's ok.
        if fields is None:
            fields = [IncludeColumn(name="is_deleted", include=False)]
        # if it's a list of strings or a list of include columns, it's fine.
        return super().find_one(filter=filters, fields=fields, skip=skip)

    @classmethod
    def find_many(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[T]:
        if filter is None:
            filters = {}
        elif isinstance(filter, dict) and "is_deleted" not in filter:
            filters = filter | {"is_deleted": False}
        else:
            filters = filter
        # if it were an instance of Document, it would either
        # already be using the default value
        # or the user would have set his own. Either way, it's ok.
        if fields is None:
            fields = [IncludeColumn(name="is_deleted", include=False)]
        # if it's a list of strings, it's fine.
        # TODO: if it's a list of include columns, we should check whether
        # it's removals only and add the is_deleted removal.
        result = super().find_many(
            filter=filters, fields=fields, sort=sort, skip=skip, limit=limit
        )
        return result  # type: ignore
