from typing import Literal, TypeVar

from .client import Client

T = TypeVar("T")


class CRUDOperationResult:
    matched_count: int


class REDB:
    client: Client | None

    @classmethod
    def get(cls):
        if cls.client is None:
            raise RuntimeError("DB not initialized.")
        return cls.client

    @classmethod
    def init_db(cls, backend: Literal["json", "mongo"], kwargs):
        cls.client = Client.from_prefix(backend, **kwargs)
