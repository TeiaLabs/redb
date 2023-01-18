from typing import Literal

from redb.interface import Client
from redb.interface.configs import JSONConfig, MilvusConfig, MongoConfig


def check_config(
    config,
    base_class,
) -> bool:
    return isinstance(config, base_class) or isinstance(config, dict)


class RedB:
    """Client singleton."""

    _client: Client | None = None
    _client_name: str | None = None

    @classmethod
    def get_client(cls) -> Client:
        if cls._client is None:
            raise RuntimeError("Client not setup. Call setup() first.")
        return cls._client

    @classmethod
    def get_client_name(cls) -> str:
        if cls._client_name is None:
            raise RuntimeError("Client not setup. Call setup() first.")
        return cls._client_name

    @classmethod
    def setup(
        cls,
        config,
        backend: Literal["json", "mongo"] | None = None,
    ) -> None:
        if backend is None and isinstance(config, dict):
            raise ValueError("Cannot determine client type from backend and config")
        elif backend == "json" or check_config(config, JSONConfig):
            from redb.json_system import JSONClient

            cls._client = JSONClient(config)
            cls._client_name = "json"

        elif backend == "mongo" or check_config(config, MongoConfig):
            from redb.mongo_system import MongoClient

            cls._client = MongoClient(config)
            cls._client_name = "mongo"

        elif backend == "milvus" and check_config(config, MilvusConfig):
            from redb.milvus_system import MilvusClient

            cls._client = MilvusClient(config)
            cls._client_name = "milvus"

        else:
            raise ValueError(f"Backend not found for config type: {type(config)!r}.")