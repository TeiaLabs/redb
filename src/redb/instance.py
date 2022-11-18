from typing import Any, Literal, TypeVar

from ..json_system import JSONClient
from ..mongo_system import MongoClient
from .interfaces import Client

C = TypeVar("C", bound=Client)


def execute_collection_function(cls, func_name: str, **kwargs) -> Any:
    client = RedB.get_client()
    database = cls.__database__ or client.get_default_database()
    collection_name = cls.__name__

    collection = client[database][collection_name]
    return getattr(collection, func_name)(**kwargs)


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
