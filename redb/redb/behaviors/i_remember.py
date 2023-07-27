from datetime import datetime
from typing import Any, Dict, Optional, TypeVar, Type

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
from redb.interface.results import DeleteManyResult, ReplaceOneResult
from ..interface.errors import DocumentNotFound, UniqueConstraintViolation
from ..interface.fields import ClassField, Direction, Field, SortColumn
from ..interface.results import DeleteOneResult, InsertOneResult, UpdateOneResult

T = TypeVar("T", bound="IRememberDoc")
DOCUMENT_FIELDS = {"id", "created_at", "created_by"}
HISTORY_FIELDS = {"ref_id", "version", "retired_by", "retired_at"}


class IRememberDoc(Document):
    ref_id: str = ""
    version: int = 0
    retired_at: Optional[datetime] = None

    def __init__(self, **data):
        calculate_hash = False
        if "id" in data:
            data["_id"] = data.pop("id")
        if "_id" not in data:
            data["_id"] = None
            calculate_hash = True

        data = self.update_kwargs(data)
        if calculate_hash:
            data["_id"] = self.get_hash(data)

        data["ref_id"] = data["_id"]
        super().__init__(**data)

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

    @classmethod
    def _historical_insert_one(
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
    def history_collection_name(cls):
        return f"{cls.collection_name()}-history"

    def dict(self, ignored_history_fields: bool = True, *args, **kwargs) -> dict:
        if ignored_history_fields:
            if "exclude" in kwargs and kwargs["exclude"] is not None:
                kwargs["exclude"] |= HISTORY_FIELDS
            else:
                kwargs["exclude"] = HISTORY_FIELDS

        out = super().dict(*args, **kwargs)
        return out

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        all_fields = super().get_hashable_fields()
        return list(
            filter(lambda x: x.model_field.name not in HISTORY_FIELDS, all_fields)
        )

    @classmethod
    def historical_find_one(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> T:
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
    def historical_find_many(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = SortColumn(name="version", direction=Direction.DESCENDING),
        skip: int = 0,
        limit: int = 0,
    ) -> list[T]:
        """
        Find many on the history collection.

        Allow users to query for previous revisions.
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
    def delete_history(
        cls,
        filter: OptionalDocumentData = None,
    ) -> DeleteManyResult:
        collection = cls._get_history_collection()
        filter = _format_document_data(filter)
        return collection.delete_many(
            cls=cls,
            filter=filter,
        )

    @classmethod
    def historical_update_one(
        cls,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
        user_info: Any = None,
    ) -> UpdateOneResult:
        # TODO: this desperately needs a transaction
        assert not upsert and operator == "$set" and not allow_new_fields
        # TODO: fix these missing behaviors or raise appropriate errors
        original_doc = super().find_one(filter=filter)
        new_history = cls._build_history_from_ref(user_info, original_doc)
        cls._historical_insert_one(new_history)
        original_obj = original_doc.dict()
        original_obj.pop("_id")
        if isinstance(update, dict):
            new_obj = original_obj | update
        else:
            new_obj = original_obj | update.dict()
        new_doc = cls(**new_obj)
        cls.delete_one(filter=filter)
        cls.insert_one(new_doc)
        update_result = UpdateOneResult(matched_count=1, modified_count=1, upserted_id=None)
        return update_result

    @classmethod
    def historical_replace_one(
        cls, filter: DocumentData, replacement: DocumentData, user_info: Any = None
    ) -> ReplaceOneResult:
        # TODO: wrap in ACID transaction
        original_doc = super().find_one(filter=filter)
        new_history = cls._build_history_from_ref(user_info, original_doc)
        cls._historical_insert_one(new_history)
        if isinstance(replacement, dict):
            new_obj = replacement
        else:
            new_obj = replacement.dict()
        new_obj.pop("_id", "")
        cls.delete_one(filter=filter)
        new_doc = cls(**new_obj)
        new_doc.insert()
        return ReplaceOneResult(matched_count=1, modified_count=1, upserted_id=None)

    @classmethod
    def historical_delete_one(
        cls,
        filter: DocumentData,
        user_info: Any = None,
    ) -> DeleteOneResult:
        original_doc = super().find_one(filter=filter)
        new_history = cls._build_history_from_ref(user_info, original_doc)
        delete_result = cls.delete_one(filter={"_id": original_doc.id})
        cls._historical_insert_one(new_history)
        return delete_result

    @classmethod
    def _build_history_from_ref(
        cls,
        user_info: Any,
        referenced_doc: "IRememberDoc",
    ) -> Dict:
        history_filter = {"ref_id": referenced_doc.id}
        try:
            histories = cls.historical_find_many(filter=history_filter, fields=["version"], limit=1, sort=SortColumn(name="version", direction=Direction.DESCENDING))
            history = histories[0]
            version = history["version"] + 1  # type: ignore
        except (DocumentNotFound, IndexError):
            version = 1

        new_history = referenced_doc.dict(
            ignored_history_fields=False,
            exclude={"id"},
            exclude_none=True,
        )
        new_history["version"] = version
        new_history["retired_by"] = (
            user_info.dict() if hasattr(user_info, "dict") else str(user_info)
        )
        new_history["retired_at"] = pytz.UTC.localize(datetime.utcnow()).isoformat()
        new_history["ref_id"] = referenced_doc.id
        new_history["_id"] = f"{referenced_doc.id}_v{version}"
        return new_history
