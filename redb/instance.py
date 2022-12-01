from typing import TypeVar

from .interfaces import Client
from .json_system import JSONClient, JSONConfig
from .mongo_system import MongoClient, MongoConfig

C = TypeVar("C", bound=Client)
CONFIGS = TypeVar("CONFIGS", MongoConfig, JSONConfig)


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
    def setup(cls, config: CONFIGS) -> None:
        if isinstance(config, JSONConfig):
            cls.client = JSONClient(config)
        elif isinstance(config, MongoConfig):
            cls.client = MongoClient(config)
        else:
            raise ValueError(f"Backend not found for config type: {type(config)!r}.")
