from typing import Literal, TypeVar

from .interfaces import Client
from .json_system import JSONClient
from .mongo_system import MongoClient

C = TypeVar("C", bound=Client)


class RedB:
    """Client singleton."""

    client: C | None = None

    @classmethod
    def get_client(cls, client_name) -> Client:
        if cls.client is None:
            raise RuntimeError("Client not setup. Call setup() first.")

        incompatible_error_message = (
            f"Current client does not match required client: {client_name}"
        )
        if isinstance(cls.client, MongoClient) and client_name != "mongo":
            raise ValueError(incompatible_error_message)
        elif isinstance(cls.client, JSONClient) and client_name != "json":
            raise ValueError(incompatible_error_message)
        return cls.client

    @classmethod
    def setup(cls, backend: Literal["json", "mongo"], *args, **kwargs) -> None:
        if backend == "json":
            cls.client = JSONClient(*args, **kwargs)
        elif backend == "mongo":
            cls.client = MongoClient(*args, **kwargs)
        else:
            raise ValueError(f"Backend {backend!r} not supported.")
