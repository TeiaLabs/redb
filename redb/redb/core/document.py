from datetime import datetime
from typing import Any, Dict, Type, TypeAlias, TypeVar, Union, Sequence, cast

from redb.interface.errors import CannotUpdateIdentifyingField
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
    created_at: datetime = Field(default_factory=datetime.utcnow)  # type: ignore
    updated_at: datetime = Field(default_factory=datetime.utcnow)  # type: ignore

    class Config:
        json_encoders = {
            datetime: lambda d: d.isoformat(),
            DBRef: lambda ref: dict(ref.as_doc()),
            ObjectId: str,
        }
        smart_union = True

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
    ) -> list[T]:
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
        return collection.insert_one(
            cls=self.__class__,
            data=data,
        )

    @classmethod
    def insert_one(
        cls: Type[T],
        data: DocumentData,
    ) -> InsertOneResult:
        _validate_fields(cls, data)

        collection = Document._get_collection(cls)
        data = _format_document_data(data)
        return collection.insert_one(
            cls=cls,
            data=data,
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
        return collection.insert_many(
            cls=cls,
            data=instances,
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
        result = collection.insert_many(
            cls=cls,
            data=data,
        )
        return result

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
        if operator is not None:
            update = {operator: update}

        return collection.update_one(
            cls=self.__class__,
            filter=filter,
            update=update,
            upsert=upsert,
        )

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
        filters = _format_document_data(filter)
        update_data = _format_document_data(update)
        hashable_field_attr_names = set(
            x.model_field.name for x in cls.get_hashable_fields()
        )
        for field in update_data.keys():
            if field in hashable_field_attr_names:
                m = f"Cannot update hashable field {field} on {cls.__name__}"
                raise CannotUpdateIdentifyingField(m)
        if operator is not None:
            update_data = {operator: update_data}
        result = collection.update_one(
            cls=cls,
            filter=filters,
            update=update_data,
            upsert=upsert,
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
        if operator is not None:
            update = {operator: update}

        return collection.update_many(
            cls=cls,
            filter=filter,
            update=update,
            upsert=upsert,
        )

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
