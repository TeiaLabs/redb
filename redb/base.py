import hashlib
import pickle
from typing import Any, Type, TypeVar

from pydantic import BaseModel
from pydantic.main import ModelMetaclass

from .instance import RedB, process_indices
from .interfaces import Collection

T = TypeVar("T", bound="Collection")


class BaseMetaclass(ModelMetaclass):
    def __new__(
        cls: Type[Collection],
        clsname: str,
        bases: list[Type[Collection]],
        attrs: dict[str, Any],
    ) -> Collection:
        if clsname not in RedB._processed_classes:
            process_indices(clsname, attrs)
            RedB._processed_classes.add(clsname)

        return super().__new__(cls, clsname, bases, attrs)


class BaseCollection(Collection, BaseModel, metaclass=BaseMetaclass):
    __database_name__: str | None = None
    __client_name__: str | None = None

    def __init__(self, collection_name: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        object.__setattr__(
            self, "__collection_name__", collection_name or self.__class__.__name__
        )

    @classmethod
    def collection_name(cls: Type[T]) -> str:
        return cls.__name__.lower()

    def get_hash(self) -> str:
        hashses = []
        for field in self.__fields__:
            value = self.__getattribute__(field)
            key_field_hash = hashlib.md5(field.encode("utf8")).hexdigest()
            val_field_hash = hashlib.md5(pickle.dumps(value)).hexdigest()
            hashses += [key_field_hash, val_field_hash]

        hex_digest = hashlib.sha256("".join(hashses).encode("utf-8")).hexdigest()
        return hex_digest

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"


class DocumentMetaclass(ModelMetaclass):
    def __new__(
        cls: Type[Collection],
        clsname: str,
        bases: list[Type[Collection]],
        attrs: dict[str, Any],
    ) -> Collection:
        class_type = super().__new__(cls, clsname, bases, attrs)
        if RedB._sub_classes is None:
            RedB._sub_classes = []

        if clsname not in RedB._processed_classes:
            RedB._sub_classes.append(class_type)
            process_indices(clsname, attrs)
            RedB._processed_classes.add(clsname)

        return class_type


class Document(Collection, metaclass=DocumentMetaclass):
    pass
