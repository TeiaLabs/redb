import hashlib
import pickle
from typing import Type, TypeVar

from pydantic import BaseModel

from .interfaces import Collection, CompoundIndice, Indice

T = TypeVar("T", bound="Collection")


class BaseCollection(Collection, BaseModel):
    __database_name__: str | None = None
    __client_name__: str | None = None

    @classmethod
    def get_indices(cls) -> list[Indice | CompoundIndice]:
        return []

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
