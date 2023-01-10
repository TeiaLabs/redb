import hashlib
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar

from pydantic import BaseModel
from pydantic.fields import ModelField
from pydantic.main import ModelMetaclass

from .instance import RedB
from .interfaces import (
    BulkWriteResult,
    ClassField,
    Collection,
    CompoundIndex,
    DeleteManyResult,
    DeleteOneResult,
    Field,
    IncludeDBColumn,
    Index,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    ReplaceOneResult,
    SortDBColumn,
    UpdateManyResult,
    UpdateOneResult,
)

T = TypeVar("T", bound=Collection)


class DocumentMetaclass(ModelMetaclass):
    def __getattribute__(cls_or_self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError as e:
            if hasattr(cls_or_self, "__fields__"):
                if name not in cls_or_self.__fields__:
                    raise e

                return ClassField(
                    model_field=cls_or_self.__fields__[name],
                    base_class=cls_or_self,
                )


class BaseDocument(BaseModel, metaclass=DocumentMetaclass):
    __database_name__: ClassVar[Optional[str]] = None

    @staticmethod
    def _get_driver_collection(
        instance_or_class: Type["Document"] | "Document",
    ) -> Any:
        if isinstance(instance_or_class, type):
            collection_name = instance_or_class.__name__
        else:
            collection_name = (
                instance_or_class.__class__.__name__
                if not hasattr(instance_or_class, "_collection_name")
                else object.__getattribute__(instance_or_class, "_collection_name")
            )

        database_name = instance_or_class.__database_name__

        client = RedB.get_client()
        database = (
            client.get_database(database_name)
            if database_name
            else client.get_default_database()
        )

        client_name = RedB.get_client_name()
        if client_name == "mongo":
            return database._get_driver_database()[collection_name]
        elif client_name == "json":
            return database._get_driver_database() / collection_name
        elif client_name == "milvus":
            pass

    @staticmethod
    def _get_system_collection(driver_collection: Collection) -> "Collection":
        from .json_system import JSONCollection
        from .mongo_system import MongoCollection

        client_name = RedB.get_client_name()
        if client_name == "mongo":
            return MongoCollection(driver_collection)
        elif client_name == "json":
            return JSONCollection(driver_collection)
        elif client_name == "milvus":
            pass

    @staticmethod
    def _get_collection(instance_or_class: Type["Document"] | "Document"):
        driver_collection = Document._get_driver_collection(instance_or_class)
        system_collection = Document._get_system_collection(driver_collection)
        return system_collection

    def dict(self, keep_id: bool = False, *args, **kwargs) -> Dict:
        out = super().dict(*args, **kwargs)
        if not keep_id:
            out["id"] = self.get_hash()
        return out

    @classmethod
    def collection_name(cls: Type[T]) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return []

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return []

    @staticmethod
    def hash_function(string: str) -> str:
        return hashlib.sha3_256(string.encode("utf-8")).hexdigest()

    def get_hash(self) -> str:
        return Document._get_hash(self)

    @staticmethod
    def _get_hash(self) -> str:
        stringfied_fields = []
        # for field in Document.get_hashable_fields(self):
        #     if BaseModel in field.type_.mro():
        #         stringfied_fields.append(Document._get_hash(getattr(self, field.alias)))
        #     else:
        #         stringfied_fields.append(str(getattr(self, field.alias)))

        string = "".join(stringfied_fields)
        return Document.hash_function(string)

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"


class Document(BaseDocument):
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def id(self):
        return self.get_hash()

    def dict(self, *args, **kwargs) -> Dict:
        out = super().dict(*args, **kwargs)
        out["created_at"] = str(out["created_at"])
        out["updated_at"] = str(out["updated_at"])
        return out

    @classmethod
    def create_indices(cls: Type["Document"]) -> None:
        collection = Document._get_collection(cls)
        indices = cls.get_indices()
        for indice in indices:
            if isinstance(indice, Index):
                indice = CompoundIndex(
                    fields=[indice.field],
                    name=indice.name,
                    unique=indice.unique,
                    direction=indice.direction,
                )

            collection.create_indice(indice)

    @classmethod
    def find(
        cls: Type["Document"],
        filter: Optional["Document"] = None,
        fields: Optional[list[IncludeDBColumn] | list[str]] = None,
        sort: Optional[list[SortDBColumn] | SortDBColumn] = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list["Document"]:
        collection = Document._get_collection(cls)
        results = collection.find(filter, fields, sort, skip, limit)
        if not results:
            return results

        return [cls(**result) for result in results]

    @classmethod
    def find_vectors(
        cls: Type["Document"],
        column: Optional[str] = None,
        filter: Optional["Document"] = None,
        sort: Optional[list[SortDBColumn] | SortDBColumn] = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list["Document"]:
        collection = Document._get_collection(cls)
        results = collection.find_vectors(column, filter, sort, skip, limit)
        if not results:
            return results

        return [cls(**result) for result in results]

    @classmethod
    def find_one(
        cls: Type["Document"],
        filter: Optional["Document"] = None,
        skip: int = 0,
    ) -> "Document":
        collection = Document._get_collection(cls)
        result = collection.find_one(filter, skip)
        if not result:
            return result

        return cls(**result)

    @classmethod
    def distinct(
        cls: Type["Document"],
        key: str,
        filter: Optional["Document"] = None,
    ) -> list["Document"]:
        collection = Document._get_collection(cls)
        results = collection.distinct(key, filter)
        if not results:
            return results

        return [cls(**result) for result in results]

    @classmethod
    def count_documents(
        cls: Type["Document"],
        filter: Optional["Document"] = None,
    ) -> int:
        collection = Document._get_collection(cls)
        return collection.count_documents(filter)

    @classmethod
    def bulk_write(
        cls: Type["Document"],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = Document._get_collection(cls)
        return collection.bulk_write(operations)

    def insert_one(data: "Document") -> InsertOneResult:
        collection = Document._get_collection(data)
        return collection.insert_one(data)

    @classmethod
    def insert_vectors(
        cls: Type["Document"],
        data: Dict[str, list[Any]],
    ) -> InsertManyResult:
        collection = Document._get_collection(cls)
        return collection.insert_vectors(cls, data)

    @classmethod
    def insert_many(
        cls: Type["Document"],
        data: list["Document"],
    ) -> InsertManyResult:
        collection = Document._get_collection(cls)
        return collection.insert_many(data)

    def replace_one(
        filter: "Document",
        replacement: "Document",
        upsert: bool = False,
    ) -> ReplaceOneResult:
        collection = Document._get_collection(filter)
        return collection.find_vectors(filter, replacement, upsert)

    def update_one(
        filter: "Document",
        update: "Document",
        upsert: bool = False,
    ) -> UpdateOneResult:
        collection = Document._get_collection(filter)
        return collection.update_one(filter, update, upsert)

    @classmethod
    def update_many(
        cls: Type["Document"],
        filter: "Document",
        update: "Document",
        upsert: bool = False,
    ) -> UpdateManyResult:
        collection = Document._get_collection(cls)
        return collection.update_many(filter, update, upsert)

    def delete_one(filter: "Document") -> DeleteOneResult:
        collection = Document._get_collection(filter)
        return collection.delete_one(filter)

    @classmethod
    def delete_many(
        cls: Type["Document"],
        filter: "Document",
    ) -> DeleteManyResult:
        collection = Document._get_collection(cls)
        return collection.delete_many(filter)
