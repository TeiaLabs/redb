from datetime import datetime
from typing import Any, Dict, Type, TypeVar

from redb.interface.fields import (
    CompoundIndex,
    Field,
    IncludeColumn,
    Index,
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

DocumentData = TypeVar(
    "DocumentData",
    bound="Document" | Dict[str, Any] | None,
)
OptionalDocumentData = TypeVar(
    "OptionalDocumentData",
    bound="Document" | Dict[str, Any] | None,
)
IncludeColumns = TypeVar(
    "IncludeColumns",
    bound=list[IncludeColumn] | list[str] | None,
)
SortColumns = TypeVar(
    "SortColumns",
    bound=list[SortColumn] | SortColumn | None,
)


class Document(BaseDocument):
    id: str = Field(alias="_id")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {datetime: lambda d: d.isoformat()}

    def __init__(self, **data: Any) -> None:
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

    def dict(self, *args, **kwargs) -> dict:
        if "by_alias" not in kwargs:
            kwargs["by_alias"] = True
        out = super().dict(*args, **kwargs)
        return _apply_encoders(out, self.__config__.json_encoders)

    @classmethod
    def create_indexes(cls: Type["Document"]) -> None:
        collection = Document._get_collection(cls)
        indexes = cls.get_indexes()
        for index in indexes:
            index = _format_index(index)
            collection.create_index(index)

    @classmethod
    def find(
        cls: Type["Document"],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list["Document"]:
        collection = Document._get_collection(cls)
        return_cls = _get_return_cls(cls, fields)
        filter = _format_document_data(filter)
        fields = _format_fields(fields)
        sort = _format_sort(sort)
        return collection.find(
            cls=cls,
            return_cls=return_cls,
            filter=filter,
            fields=fields,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    @classmethod
    def find_one(
        cls: Type["Document"],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> "Document":
        collection = Document._get_collection(cls)
        return_cls = _get_return_cls(cls, fields)
        filter = _format_document_data(filter)
        fields = _format_fields(fields)
        return collection.find_one(
            cls=cls,
            return_cls=return_cls,
            filter=filter,
            skip=skip,
        )

    @classmethod
    def distinct(
        cls: Type["Document"],
        key: str,
        filter: OptionalDocumentData = None,
    ) -> list["Document"]:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.distinct(
            cls=cls,
            key=key,
            filter=filter,
        )

    @classmethod
    def count_documents(
        cls: Type["Document"],
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
        cls: Type["Document"],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = Document._get_collection(cls)
        return collection.bulk_write(
            cls=cls,
            operations=operations,
        )

    def insert_one(data: DocumentData) -> InsertOneResult:
        _validate_fields(data.__class__, data)

        collection = Document._get_collection(data)
        data = _format_document_data(data)
        return collection.insert_one(
            cls=data.__class__,
            data=data,
        )

    @classmethod
    def insert_vectors(
        cls: Type["Document"],
        data: Dict[str, list[Any]],
    ) -> InsertManyResult:
        collection = Document._get_collection(cls)
        keys = list(data.keys())
        values_size = len(data[keys[0]])
        instances = [None] * values_size
        instances = [{key: data[key][i] for key in keys} for i in range(values_size)]
        return collection.insert_many(
            cls=cls,
            data=instances,
        )

    @classmethod
    def insert_many(
        cls: Type["Document"],
        data: list[DocumentData],
    ) -> InsertManyResult:
        [_validate_fields(cls, val) for val in data]

        collection = Document._get_collection(cls)
        data = [_format_document_data(val) for val in data]
        return collection.insert_many(
            cls=cls,
            data=data,
        )

    def replace_one(
        filter: DocumentData,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        if not allow_new_fields:
            _validate_fields(filter.__class__, replacement)

        collection = Document._get_collection(filter)
        filter = _format_document_data(filter)
        replacement = _format_document_data(replacement)
        return collection.replace_one(
            cls=filter.__class__,
            filter=filter,
            replacement=replacement,
            upsert=upsert,
        )

    def update_one(
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        if not allow_new_fields:
            _validate_fields(filter.__class__, update)

        collection = Document._get_collection(filter)
        filter = _format_document_data(filter)
        update = _format_document_data(update)
        return collection.update_one(
            cls=filter.__class__,
            filter=filter,
            update=update,
            upsert=upsert,
        )

    @classmethod
    def update_many(
        cls: Type["Document"],
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> UpdateManyResult:
        if not allow_new_fields:
            _validate_fields(cls, update)

        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        update = _format_document_data(update)
        return collection.update_many(
            cls=cls,
            filter=filter,
            update=update,
            upsert=upsert,
        )

    def delete_one(filter: DocumentData) -> DeleteOneResult:
        collection = Document._get_collection(filter)
        filter = _format_document_data(filter)
        return collection.delete_one(
            cls=filter.__class__,
            filter=filter,
        )

    @classmethod
    def delete_many(
        cls: Type["Document"],
        filter: DocumentData,
    ) -> DeleteManyResult:
        collection = Document._get_collection(cls)
        filter = _format_document_data(filter)
        return collection.delete_many(
            cls=cls,
            filter=filter,
        )


def _apply_encoders(obj, encoders):
    obj_type = type(obj)
    if obj_type == list:
        obj = [_apply_encoders(val, encoders) for val in obj]
    elif obj_type == set:
        obj = {_apply_encoders(val, encoders) for val in obj}
    elif obj_type == tuple:
        obj = (_apply_encoders(val, encoders) for val in obj)
    elif obj_type == dict:
        obj = {
            _apply_encoders(key, encoders): _apply_encoders(val, encoders)
            for key, val in obj.items()
        }
    elif obj_type in encoders:
        encoding = encoders[obj_type]
        obj = encoding(obj) if callable(encoding) else encoding
    return obj


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
    fields: IncludeColumns = None,
) -> Type[Document | dict]:
    if not fields:
        return cls

    return_type = cls
    if not all(v.required and k in fields for k, v in cls.__fields__.items()):
        return_type = dict

    return return_type


def _format_fields(fields: IncludeColumns) -> dict[str, bool]:
    formatted_fields = fields
    if fields is not None:
        if isinstance(fields[0], str):
            formatted_fields = {field: True for field in fields}
        else:
            formatted_fields = {field.name: field.include for field in fields}

    return formatted_fields


def _format_sort(sort: SortColumns) -> dict[tuple[str, str | int]]:
    formatted_sort = sort
    if sort is not None:
        if isinstance(sort, list):
            formatted_sort = [(field.name, field.direction) for field in sort]
        else:
            formatted_sort = [(sort.name, sort.direction)]

    return formatted_sort


def _format_document_data(data: OptionalDocumentData):
    if data is None:
        return None
    if isinstance(data, Document):
        return data.dict()
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
