from typing import Literal, TypeVar

from ..json_system import JSONClient
from ..mongo_system import MongoClient
from .interfaces import Client

C = TypeVar("C", bound=Client)


class RedB:
    """Client singleton."""

    client: C | None = None

    @classmethod
    def get_client(cls) -> Client:
        if cls.client is None:
            raise RuntimeError
        return cls.client

    @classmethod
    def setup(cls, backend: Literal["json", "mongo"], *args, **kwargs) -> None:
        if backend == "json":
            cls.client = JSONClient(*args, **kwargs)
        elif backend == "mongo":
            cls.client = MongoClient(*args, **kwargs)
        else:
            raise ValueError(f"Backend {backend!r} not supported.")
