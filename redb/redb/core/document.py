from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Sequence, Type, TypeAlias, TypeVar, Union, cast

import pytz
from pymongo.errors import DuplicateKeyError

from redb.interface.errors import (
    CannotUpdateIdentifyingField,
    UniqueConstraintViolation,
    UnsupportedOperation,
)
from redb.interface.fields import (
    CompoundIndex,
    DBRef,
    Field,
    IncludeColumn,
    Index,
    ObjectId,
    PyMongoOperations,
    SortColumn,
)
from redb.interface.results import (
    BulkWriteResult,
    DeleteManyResult,
    DeleteOneResult,
    InsertManyResult,
    InsertOneResult,
    ReplaceOneResult,
    UpdateManyResult,
    UpdateOneResult,
)

from .base import BaseDocument

DocumentData: TypeAlias = Union["Document", Dict[str, Any]]
IncludeColumns: TypeAlias = list[IncludeColumn] | list[str] | None
OptionalDocumentData: TypeAlias = Union["Document", dict[str, Any], None]
SortColumns: TypeAlias = list[SortColumn] | SortColumn | None
T = TypeVar("T", bound="Document")


class Document(BaseDocument):
    id: str = Field(alias="_id")  # type: ignore
    created_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))  # type: ignore
    updated_at: datetime = Field(default_factory=lambda: datetime.now(pytz.UTC))  # type: ignore

    class Config:
        json_encoders = {
            datetime: lambda d: d.isoformat(),
            DBRef: lambda ref: dict(ref.as_doc()),
            Path: str,
            ObjectId: str,
        }
        smart_union = True

    def __init__(self, **data: Any) -> None:
        # TODO rewrite this using root_validator(pre=True) and root_validator(pre=False)
        calculate_hash = False
        if "id" in data:
            data["_id"] = data.pop("id")
        if "_id" not in data:
            data["_id"] = None
            calculate_hash = True

        data = self.update_kwargs(data)
        if calculate_hash:
            data["_id"] = self.get_hash(data)
        super().__init__(**data)

    @classmethod
    def create_indexes(cls: Type[T]) -> None:
        collection = Document._get_collection(cls)
        indexes = cls.get_indexes()
        for index in indexes:
            index = _format_index(index)
            collection.create_index(index)

    def find(
        self: T,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> T:
        collection = Document._get_collection(self.__class__)
        filter = _format_document_data(self)
        formatted_fields = _format_fields(fields)
        return_cls = _get_return_cls(self.__class__, fields)
        return collection.find_one(
            cls=self.__class__,
            return_cls=return_cls,
            filter=filter,
            skip=skip,
            fields=formatted_fields,
        )

    @classmethod
    def find_one(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> T:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        formatted_fields = _format_fields(fields)
        return_cls = _get_return_cls(cls, formatted_fields)
        return collection.find_one(
            cls=cls,
            return_cls=return_cls,
            filter=filter,
            skip=skip,
            fields=formatted_fields,
        )

    @classmethod
    def find_many(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
        iterate: bool = False,
        batch_size: int | None = None,
    ) -> list[T]:
        if iterate and batch_size is not None:
            msg = "'iterate' cannot be used with 'batch_size'. Batched find_many is already an iterable."
            raise UnsupportedOperation(msg)

        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        formatted_fields = _format_fields(fields)
        return_cls = _get_return_cls(cls, formatted_fields)
        sort_order = _format_sort(sort)
        return collection.find(
            cls=cls,
            return_cls=return_cls,
            filter=filter,
            fields=formatted_fields,
            sort=sort_order,
            skip=skip,
            limit=limit,
            iterate=iterate,
            batch_size=batch_size,
        )

    @classmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: OptionalDocumentData = None,
    ) -> list[Any]:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.distinct(
            cls=cls,
            key=key,
            filter=filter,
        )

    @classmethod
    def count_documents(
        cls: Type[T],
        filter: OptionalDocumentData = None,
    ) -> int:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.count_documents(
            cls=cls,
            filter=filter,
        )

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = Document._get_collection(cls)
        return collection.bulk_write(
            cls=cls,
            operations=operations,
        )

    def insert(self: T) -> InsertOneResult:
        collection = Document._get_collection(self.__class__)
        data = _format_document_data(self)
        try:
            return collection.insert_one(
                cls=self.__class__,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=self.collection_name()
            )

    @classmethod
    def insert_one(
        cls: Type[T],
        data: DocumentData,
    ) -> InsertOneResult:
        _validate_fields(cls, data)

        collection = Document._get_collection(cls)
        data = _format_document_data(data)
        try:
            return collection.insert_one(
                cls=cls,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=cls.collection_name()
            )

    @classmethod
    def insert_vectors(
        cls: Type[T],
        data: Dict[str, list[Any]],
    ) -> InsertManyResult:
        collection = Document._get_collection(cls)
        keys = list(data.keys())
        values_size = len(data[keys[0]])
        instances = [None] * values_size
        instances = [{key: data[key][i] for key in keys} for i in range(values_size)]
        try:
            return collection.insert_many(
                cls=cls,
                data=instances,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=cls.collection_name()
            )

    @classmethod
    def insert_many(
        cls: Type[T],
        data: Sequence[DocumentData],
    ) -> InsertManyResult:
        for val in data:
            _validate_fields(cls, val)
        collection = Document._get_collection(cls)
        data = [_format_document_data(val) for val in data]
        try:
            result = collection.insert_many(
                cls=cls,
                data=data,
            )
            return result
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=cls.collection_name()
            )

    def replace(
        self: T,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        if not allow_new_fields:
            _validate_fields(self.__class__, replacement)

        collection = Document._get_collection(self.__class__)
        filter = _format_document_data(self)
        replacement = _format_document_data(replacement)
        return collection.replace_one(
            cls=self.__class__,
            filter=filter,
            replacement=replacement,
            upsert=upsert,
        )

    @classmethod
    def replace_one(
        cls: Type[T],
        filter: DocumentData,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        if not allow_new_fields:
            _validate_fields(filter.__class__, replacement)

        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        replacement = _format_document_data(replacement)
        return collection.replace_one(
            cls=cls,
            filter=filter,
            replacement=replacement,
            upsert=upsert,
        )

    def update(
        self,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        if not allow_new_fields:
            _validate_fields(self.__class__, update)

        collection = Document._get_collection(self.__class__)
        filter = _format_document_data(self)
        update = _format_document_data(update)

        if not upsert:
            filter = _optimize_filter(self.__class__, filter)

        _raise_if_updating_hashable(self.__class__, update)
        if operator is not None:
            update = {operator: update}

        result = collection.update_one(
            cls=self.__class__,
            filter=filter,
            update=update,
            upsert=upsert,
        )
        collection.update_one(
            cls=self.__class__,
            filter=filter,
            update={"$set": {"updated_at": datetime.now(pytz.UTC).isoformat()}},
        )
        return result

    @classmethod
    def update_one(
        cls,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        if not allow_new_fields:
            _validate_fields(cls, update)

        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        update_data = _format_document_data(update)

        if not upsert:
            filter = _optimize_filter(cls, filter)

        _raise_if_updating_hashable(cls, update_data)
        if operator is not None:
            update_data = {operator: update_data}

        try:
            result = collection.update_one(
                cls=cls,
                filter=filter,
                update=update_data,
                upsert=upsert,
            )
            collection.update_one(
                cls=cls,
                filter=filter,
                update={"$set": {"updated_at": datetime.now(pytz.UTC).isoformat()}},
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=cls.collection_name()
            )
        return result

    @classmethod
    def update_many(
        cls: Type[T],
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
    ) -> UpdateManyResult:
        if not allow_new_fields:
            _validate_fields(cls, update)

        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        update = _format_document_data(update)

        if not upsert:
            filter = _optimize_filter(cls, filter)

        _raise_if_updating_hashable(cls, update)
        if operator is not None:
            update = {operator: update}

        try:
            result = collection.update_many(
                cls=cls,
                filter=filter,
                update=update,
                upsert=upsert,
            )
            collection.update_many(
                cls=cls,
                filter=filter,
                update={"$set": {"updated_at": datetime.now(pytz.UTC).isoformat()}},
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"], collection_name=cls.collection_name()
            )

        return result

    def delete(self: T) -> DeleteOneResult:
        collection = Document._get_collection(self.__class__)
        filter = _format_document_data(self)
        return collection.delete_one(
            cls=self.__class__,
            filter=filter,
        )

    @classmethod
    def delete_one(
        cls: Type[T],
        filter: DocumentData,
    ) -> DeleteOneResult:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.delete_one(
            cls=cls,
            filter=filter,
        )

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: DocumentData,
    ) -> DeleteManyResult:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.delete_many(
            cls=cls,
            filter=filter,
        )


def _validate_fields(cls: Type[DocumentData], data: DocumentData) -> None:
    if data is None:
        return

    if not isinstance(cls, BaseDocument):
        return

    class_fields = cls.__fields__
    for key in data:
        if key not in class_fields:
            raise ValueError(f"Key {key} is not present in the original document")


def _get_return_cls(
    cls: Type[Document],
    fields: dict[str, bool] | None = None,
) -> Type[Document | dict]:
    if not fields:
        return cls
    return_type = cls
    selected_fields = {k for k, v in fields.items() if v}
    unselected_fields = {k for k, v in fields.items() if not v}
    for v in cls.__fields__.values():
        if not v.required:
            continue
        if v.alias == "_id":
            continue
        if unselected_fields and v.alias in unselected_fields:
            return_type = dict
            break
        if selected_fields and v.alias not in selected_fields:
            return_type = dict
            break
    return return_type


def _format_fields(fields: IncludeColumns) -> dict[str, bool] | None:
    if fields is None:
        return None
    if all(isinstance(f, str) for f in fields):
        fields = cast(list[str], fields)
        formatted_fields = {field: True for field in fields}
    else:
        fields = cast(list[IncludeColumn], fields)
        formatted_fields = {field.name: field.include for field in fields}
    if any(v for v in formatted_fields.values()):
        # if you're choosing which fields you want, we'll pass _id = False
        # otherwise (if you're only choosing which ones you don't want),
        # we'll let it be included by default.
        if "_id" not in formatted_fields:
            formatted_fields["_id"] = False
    return formatted_fields


def _format_sort(sort: SortColumns) -> list[tuple[str, str | int]] | None:
    if sort is None:
        return sort
    if isinstance(sort, list):
        formatted_sort = [(field.name, field.direction.value) for field in sort]
    else:
        formatted_sort = [(sort.name, sort.direction.value)]
    return formatted_sort


def _format_document_data(data: OptionalDocumentData) -> dict[str, Any]:
    if data is None:
        return {}
    if isinstance(data, BaseDocument):
        return data.dict(by_alias=True)
    return data


def _format_index(index: Index | CompoundIndex):
    if isinstance(index, Index):
        index = CompoundIndex(
            fields=[index.field],
            name=index.name,
            unique=index.unique,
            direction=index.direction,
        )

    return index


def _raise_if_updating_hashable(cls: Type[T], update_dict: dict):
    hashable_field_attr_names = set(
        x.model_field.name for x in cls.get_hashable_fields()
    )
    for field in update_dict.keys():
        if field in hashable_field_attr_names:
            m = f"Cannot update hashable field {field!r} on {cls.__name__!r}."
            raise CannotUpdateIdentifyingField(m)


def _optimize_filter(cls: Type[T], filter: dict) -> dict:
    if "_id" in filter:
        return {"_id": filter["_id"]}
    unique_fields = set()
    indexes = [i for i in cls.get_indexes() if isinstance(i, Index)]
    for index in indexes:
        if index.unique:
            unique_fields.add(index.field.model_field.alias)
    for key, value in filter.items():
        if key in unique_fields:
            return {key: value}
    compound_indexes = [i for i in cls.get_indexes() if isinstance(i, CompoundIndex)]
    for c_index in compound_indexes:
        if c_index.unique:
            if all(k in filter for k in c_index.fields):
                return {k: filter[k] for k in c_index.fields}
    return filter
