import hashlib
from typing import Any, Callable, ClassVar, Type

from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from redb.interface.fields import ClassField, CompoundIndex, Index

from .instance import RedB

IMPORT_ERROR_MSG = (
    "%s does not seem to be installed, maybe you forgot to `pip install redb[%s]`"
)


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
    __database_name__: ClassVar[str | None] = None

    @staticmethod
    def _get_driver_collection(
        instance_or_class: Type["BaseDocument"] | "BaseDocument",
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
        elif client_name == "migo":
            return database._get_driver_database()[collection_name]

    @staticmethod
    def _get_system_collection(driver_collection):
        client_name = RedB.get_client_name()

        if client_name == "mongo":
            try:
                from redb.mongo_system import MongoCollection
            except ImportError:
                raise ImportError(IMPORT_ERROR_MSG.format("mongo_system", "mongo"))

            return MongoCollection(driver_collection)

        elif client_name == "json":
            try:
                from redb.json_system import JSONCollection
            except ImportError:
                raise ImportError(IMPORT_ERROR_MSG.format("json_system", "json"))

            return JSONCollection(driver_collection)

        elif client_name == "migo":
            try:
                from redb.migo_system import MigoCollection
            except ImportError:
                raise ImportError(IMPORT_ERROR_MSG.format("migo_system", "migo"))

            return MigoCollection(driver_collection)

        raise ValueError(f"Unknown client: {client_name}")

    @staticmethod
    def _get_collection(instance_or_class: Type["BaseDocument"] | "BaseDocument"):
        driver_collection = BaseDocument._get_driver_collection(instance_or_class)
        system_collection = BaseDocument._get_system_collection(driver_collection)
        return system_collection

    @classmethod
    def collection_name(cls: Type["BaseDocument"]) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return []

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        return [
            ClassField(model_field=field, base_class=cls)
            for field in cls.__fields__.values()
        ]

    @staticmethod
    def hash_function(string: str) -> str:
        return hashlib.sha3_256(string.encode("utf-8")).hexdigest()

    def _get_key_value_tuples_for_hash(
        self,
        fields: list[ClassField] | list[str],
        data: dict[str, Any] | None = None,
    ) -> list[tuple[str, Any]]:
        if data:
            if isinstance(fields[0], str):
                transform = lambda field: (field, data[field])
            else:
                transform = lambda field: (field.join_attrs(), field.resolve(data))
        else:
            transform = lambda field: (field.join_attrs(), field.resolve(self))

        key_val_tuples = [transform(field) for field in fields]
        return key_val_tuples

    @staticmethod
    def _assemble_hash_string(fields: list[tuple[str, Any]]) -> str:
        return "|".join([str(val) for _, val in fields])

    def get_hash(
        self,
        data: dict[str, Any] | None = None,
        use_data_fields: bool = False,
    ) -> str:
        if use_data_fields:
            fields = list(data.keys())
        else:
            fields = self.get_hashable_fields()

        if not fields:
            raise ValueError("No hashable fields found.")
        key_val_tuples = self._get_key_value_tuples_for_hash(fields, data)
        string = self._assemble_hash_string(key_val_tuples)
        return self.hash_function(string)

    def update_kwargs(self, kwargs):
        for field in self.__fields__.values():
            if field.alias not in kwargs:
                if field.required:
                    raise ValueError(
                        f"{field.alias} is missing for document {self.__class__.__name__}"
                    )
                kwargs[field.alias] = field.get_default()

        return kwargs

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"
