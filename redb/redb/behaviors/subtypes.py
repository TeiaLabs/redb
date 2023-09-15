from itertools import groupby
from typing import Type, TypeVar

from pymongo.errors import DuplicateKeyError

from redb.interface.results import InsertOneResult

from ..core import Document
from ..core.document import (
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

T = TypeVar("T", bound="SubTypedDocument")


class SubTypedDocument(Document):
    @classmethod
    def st_insert_one(
        cls: Type[T],
        data: DocumentData,
    ) -> InsertOneResult:
        if cls.__bases__[0] == (SubTypedDocument):
            raise TypeError("Cannot insert a subtype base document")
        elif cls.__bases__[0].__bases__[0] != (SubTypedDocument):
            raise TypeError("Subtype only supports one layer of inheritance")

        _validate_fields(cls, data)
        data["type"] = cls.__name__

        collection = Document._get_collection(cls.__bases__[0])
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

    def insert(self: T) -> InsertOneResult:
        if self.__class__.__bases__[0] == (SubTypedDocument):
            raise TypeError("Cannot insert a subtype  base document")
        elif self.__class__.__bases__[0].__bases__[0] != (SubTypedDocument):
            raise TypeError("Subtype only supports one layer of inheritance")

        collection = Document._get_collection(self.__class__.__bases__[0])
        data = _format_document_data(self)
        if data["type"] != self.__class__.__name__:
            raise TypeError("Data type must match the class name")
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
    def st_find_one(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ):
        if cls.__bases__[0] == (SubTypedDocument):
            collection = Document._get_collection(cls)
            filter = _format_document_data(filter)
            formatted_fields = _format_fields(fields)
            data = collection.find_one(
                cls=cls,
                return_cls=dict,
                filter=filter,
                skip=skip,
                fields=formatted_fields,
            )
            for subclass in cls.__subclasses__():
                if str(subclass.__name__) == data["type"]:
                    re_cls = subclass
            return_cls = _get_return_cls(re_cls, formatted_fields)
            return return_cls(**data)

        elif cls.__bases__[0].__bases__[0] == (SubTypedDocument):
            collection = Document._get_collection(cls.__bases__[0])
            filter = _format_document_data(filter)
            filter["type"] = cls.__name__
            formatted_fields = _format_fields(fields)
            return_cls = _get_return_cls(cls, formatted_fields)
            return collection.find_one(
                cls=cls,
                return_cls=return_cls,
                filter=filter,
                skip=skip,
                fields=formatted_fields,
            )

        else:
            raise DocumentNotFound(
                "Subtype only supports one layer of inheritance no documents found"
            )

    @classmethod
    def st_find_many(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
    ):
        if cls.__bases__[0] == (SubTypedDocument):
            collection = Document._get_collection(cls)
            filter = _format_document_data(filter)
            formatted_fields = _format_fields(fields)
            sort_order = _format_sort(sort)

            objs = collection.find(
                cls=cls,
                return_cls=dict,
                filter=filter,
                fields=formatted_fields,
                sort=sort_order,
                skip=skip,
                limit=limit,
            )
            sorted_objs = sorted(objs, key=lambda x: x["type"])
            grouped = groupby(sorted_objs, lambda x: x["type"])
            result = []
            for key, group in grouped:
                for subclass in cls.__subclasses__():
                    if str(subclass.__name__) == key:
                        re_cls = subclass
                        return_cls = _get_return_cls(re_cls, formatted_fields)

                for obj in group:
                    result.append(return_cls(**obj))

            return result

        elif cls.__bases__[0].__bases__[0] == (SubTypedDocument):
            collection = Document._get_collection(cls.__bases__[0])
            filter = _format_document_data(filter)
            filter["type"] = cls.__name__
            formatted_fields = _format_fields(fields)
            sort_order = _format_sort(sort)
            return_cls = _get_return_cls(cls, formatted_fields)
            return collection.find(
                cls=cls,
                return_cls=return_cls,
                filter=filter,
                fields=formatted_fields,
                sort=sort_order,
                skip=skip,
                limit=limit,
            )
        else:
            raise DocumentNotFound(
                "Subtype only supports one layer of inheritance no documents found"
            )
