from datetime import datetime
from typing import Any, Dict, Optional, TypeVar

import pytz
from pymongo.collection import Collection as PyMongoCollection
from pymongo.errors import DuplicateKeyError

from ..core import RedB
from ..core.document import (
    Document,
    DocumentData,
    IncludeColumns,
    OptionalDocumentData,
    SortColumns,
    _format_document_data,
    _format_fields,
    _format_sort,
    _get_return_cls,
    _validate_fields,
)
from ..interface.errors import DocumentNotFound, UniqueConstraintViolation
from ..interface.fields import ClassField, Direction, Field, IncludeColumn, SortColumn
from ..interface.results import DeleteOneResult, InsertOneResult

T = TypeVar("T")
HISTORY_FIELDS = {"version", "retired_by", "retired_at"}


class IRememberDoc(Document):
    retired_at: Optional[datetime] = None
    retired_by: Any = None
    version: int = 0

    @classmethod
    def _get_history_collection_driver(cls) -> PyMongoCollection:
        collection_name = cls.history_collection_name()
        database_name = cls.__database_name__

        client = RedB.get_client()
        database = (
            client.get_database(database_name)
            if database_name
            else client.get_default_database()
        )
        return database._get_driver_database()[collection_name]  # type: ignore

    @classmethod
    def _get_history_collection(cls):
        from redb.mongo_system import MongoCollection
        driver_collection = cls._get_history_collection_driver()
        return MongoCollection(driver_collection)

    def dict(self, ignored_history_fields: bool = True, *args, **kwargs) -> dict:
        if ignored_history_fields:
            if "exclude" in kwargs:
                kwargs["exclude"] = set(kwargs["exclude"]) | HISTORY_FIELDS
            else:
                kwargs["exclude"] = HISTORY_FIELDS
        return super().dict(*args, **kwargs)

    @classmethod
    def history_collection_name(cls):
        return f"{cls.collection_name()}-history"

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        all_fields = super().get_hashable_fields()
        return list(filter(lambda x: x.model_field.name in HISTORY_FIELDS, all_fields))

    @classmethod
    def get_history_hashable_fields(cls) -> list[ClassField]:
        hashable_fields = cls.get_hashable_fields()
        return hashable_fields + [cls.version]  # type: ignore

    @classmethod
    def find_snapshot(
        cls,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
        sort: SortColumns = [SortColumn(name="version", direction=Direction.DESCENDING)],
    ) -> "IRememberDoc" | Dict[str, Any]:
        collection = cls._get_history_collection()
        filter = _format_document_data(filter)
        formatted_fields = _format_fields(fields)
        return_cls = _get_return_cls(cls, formatted_fields)
        return collection.find_one(
            cls=cls,
            return_cls=return_cls,  # type: ignore
            filter=filter,
            skip=skip,
            fields=formatted_fields,
        )

    @classmethod
    def find_revisions(
        cls,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = [SortColumn(name="version", direction=Direction.DESCENDING)],
        skip: int = 0,
        limit: int = 0,
    ) -> list["IRememberDoc"]:
        """
        Find many on the history collection.

        Sort by version descending by default.
        """
        collection = cls._get_history_collection()
        filter = _format_document_data(filter)
        formatted_fields = _format_fields(fields)
        return_cls = _get_return_cls(cls, formatted_fields)
        sort_order = _format_sort(sort)
        return collection.find(
            cls=cls,
            return_cls=return_cls,  # type: ignore
            filter=filter,
            fields=formatted_fields,
            sort=sort_order,
            skip=skip,
            limit=limit,
        )  # type: ignore

    @classmethod
    def historical_delete_many(
        cls,
        filter: OptionalDocumentData = None,
    ) -> list["IRememberDoc"]:
        collection = cls._get_history_collection()
        filter = _format_document_data(filter)
        return collection.delete_many(
            cls=cls,
            filter=filter,
        )  # type: ignore

    @classmethod
    def _create_one_history(
        cls,
        data: DocumentData,
    ) -> InsertOneResult:
        _validate_fields(cls, data)

        collection = cls._get_history_collection()
        data = _format_document_data(data)
        try:
            return collection.insert_one(
                cls=cls,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"],  # type: ignore
                collection_name=cls.history_collection_name(),
            )

    @classmethod
    def historical_delete_one(
        cls,
        filter: DocumentData,
        user_info: Any = None,
    ) -> tuple[DeleteOneResult, InsertOneResult]:
        original_doc = super().find_one(filter=filter)
        history_filter = original_doc.dict(exclude={"id"}, exclude_none=True)
        try:
            history = cls.find_snapshot(filter=history_filter, fields=["version"])
            version = history["version"] + 1  # type: ignore
        except DocumentNotFound:
            version = 1

        new_history = original_doc.dict(
            ignored_history_fields=False,
            exclude={"id"},
            exclude_none=True,
        )
        new_history["version"] = version
        new_history["retired_by"] = user_info
        new_history["retired_at"] = str(pytz.UTC.localize(datetime.utcnow()))

        new_history["_id"] = original_doc.get_hash(
            data=new_history, use_data_fields=True
        )

        retire_result = cls.delete_one(filter=filter)
        history_insert_result = cls._create_one_history(new_history)
        return (retire_result, history_insert_result)
