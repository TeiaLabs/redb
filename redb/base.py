import hashlib
from typing import Type, TypeVar

from pydantic import BaseModel

from .interfaces import Collection, CompoundIndex, Index, Field

T = TypeVar("T", bound="Collection")


class BaseCollection(Collection, BaseModel):
    __database_name__: str | None = None
    __client_name__: str | None = None

    def dict(self, keep_id: bool = False, *args, **kwargs)-> dict:
        out = super().dict(*args, **kwargs)
        if not keep_id:
            out["_id"] = self.get_hash()
        return out

    @classmethod
    def get_indices(cls) -> list[Index | CompoundIndex]:
        return []

    @classmethod
    def collection_name(cls: Type[T]) -> str:
        return cls.__name__.lower()

    @classmethod
    def get_hashable_fields(cls) -> list[str]:
        field_names = []
        for name, field in cls.__fields__.items():
            info = field.field_info
            if hasattr(info, "hashable") and getattr(info, "hashable"):
                field_names.append(name)

        return field_names

    @classmethod
    def hash_function(cls, string: str) -> str:
        return hashlib.sha3_256(string.encode("utf-8")).hexdigest()

    def get_hash(self) -> str:
        string = "".join(str(getattr(self, k)) for k in self.get_hashable_fields())
        return self.hash_function(string)

    def __repr__(self) -> str:
        class_name = self.__class__.__name__

        # add all pydantic attributes like attr=val, attr2=val2
        attributes = ", ".join(
            f"{field}={getattr(self, field)}" for field in self.__fields__
        )

        return f"{class_name}({attributes})"
