import hashlib
from typing import Type, TypeVar

from pydantic import BaseModel
from pydantic.fields import ModelField

from .interfaces import Collection, CompoundIndex, Index

T = TypeVar("T", bound="Collection")


class BaseCollection(Collection, BaseModel):
    __database_name__: str | None = None
    __client_name__: str | None = None

    @property
    def id(self):
        return self.get_hash()

    def dict(self, keep_id: bool = False, *args, **kwargs) -> dict:
        out = super().dict(*args, **kwargs)
        if not keep_id:
            out["id"] = self.get_hash()
        return out

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return []

    @classmethod
    def collection_name(cls: Type[T]) -> str:
        return cls.__name__.lower()

    @staticmethod
    def get_hashable_fields(cls: Type[BaseModel]) -> list[ModelField]:
        fields = []
        for field in cls.__fields__.values():
            info = field.field_info
            if hasattr(info, "hashable") and getattr(info, "hashable"):
                fields.append(field)

        return fields

    @staticmethod
    def hash_function(string: str) -> str:
        return hashlib.sha3_256(string.encode("utf-8")).hexdigest()

    def get_hash(self) -> str:
        return BaseCollection._get_hash(self)

    @staticmethod
    def _get_hash(self) -> str:
        stringfied_fields = []
        for field in BaseCollection.get_hashable_fields(self):
            if BaseModel in field.type_.mro():
                stringfied_fields.append(BaseCollection._get_hash(getattr(self, field.alias)))
            else:
                stringfied_fields.append(str(getattr(self, field.alias)))
                
        string = "".join(stringfied_fields)
        return BaseCollection.hash_function(string)

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"
