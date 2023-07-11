from typing import Literal

from redb.interface.configs import (
    CONFIG_TYPE,
    JSONConfig,
    MigoConfig,
    MongoConfig,
    check_config,
)


class RedB:
    """Client singleton."""

    _clients = None
    _uris = None
    _client_name: str | None = None
    _configs: list[CONFIG_TYPE] | None = None

    @classmethod
    def add_client(cls, config: MongoConfig):
        if cls._clients is None or cls._uris is None:
            raise RuntimeError("Client not setup. Call setup() first.")

        from redb.mongo_system import MongoClient

        if config.database_uri in cls._uris:
            index = cls._uris[config.database_uri]
            return cls._clients[index]

        client = MongoClient(config)
        cls._clients.append(client)  # type: ignore
        cls._uris = {config.database_uri: len(cls._clients) - 1}

        return client

    @classmethod
    def get_client(cls, index: int = 0, uri: str = ""):
        if cls._clients is None:
            raise RuntimeError("Client not setup. Call setup() first.")

        if uri:
            if cls._uris is None:
                raise ValueError(f"Searching for URI '{uri}' with no URI configured.")
            elif uri not in cls._uris:
                raise ValueError(f"URI '{uri}' not found.")

            index = cls._uris[uri]

        return cls._clients[index]

    @classmethod
    def get_client_name(cls) -> str:
        if cls._client_name is None:
            raise RuntimeError("Client not setup. Call setup() first.")
        return cls._client_name

    @classmethod
    def get_config(cls) -> CONFIG_TYPE:
        if cls._configs is None:
            raise RuntimeError("Client not setup. Call setup() first.")
        return cls._configs[0]

    @classmethod
    def setup(
        cls,
        config: CONFIG_TYPE,
        backend: Literal["json", "mongo"] | None = None,
    ) -> None:
        if backend is None and isinstance(config, dict):
            raise ValueError("Cannot determine client type from backend and config")
        elif backend == "json" or (
            backend is None and check_config(config, JSONConfig)
        ):
            from redb.json_system import JSONClient

            cls._clients = [JSONClient(config)]
            cls._client_name = "json"

        elif backend == "mongo" or (
            backend is None and check_config(config, MongoConfig)
        ):
            from redb.mongo_system import MongoClient

            cls._clients = [MongoClient(config)]
            cls._uris = {config.database_uri: 0}
            cls._client_name = "mongo"

        elif backend == "migo" or (
            backend is None and check_config(config, MigoConfig)
        ):
            from redb.migo_system import MigoClient

            cls._clients = [MigoClient(config)]
            cls._client_name = "migo"

        else:
            raise ValueError(f"Backend not found for config type: {type(config)!r}.")

        cls._configs = [config]
