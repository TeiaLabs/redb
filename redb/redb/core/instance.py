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
    _alias: dict[str, int] = {}
    _uris: dict[str, int] = {}
    _client_name: str | None = None
    _configs: list[CONFIG_TYPE] | None = None

    @classmethod
    def add_client(
        cls, config: MongoConfig | dict | None = None, alias: str | None = None
    ):
        if cls._clients is None:
            raise RuntimeError("Client not setup. Call setup() first.")

        from redb.mongo_system import MongoClient

        if config is None:
            if alias is None:
                msg = "No config or alias informed"
                raise ValueError(msg)

            if alias not in cls._alias:
                msg = f"Alias '{alias}' not found"
                raise ValueError(msg)

            alias_index = cls._alias[alias]
            return cls._clients[alias_index]

        if isinstance(config, dict):
            database_uri = config["database_uri"]
        else:
            database_uri = config.database_uri

        alias_index = None
        if alias is not None and alias in cls._alias:
            alias_index = cls._alias[alias]

        uri_index = None
        if database_uri in cls._uris:
            uri_index = cls._uris[database_uri]

        if (
            alias_index is not None
            and uri_index is not None
            and alias_index != uri_index
        ):
            msg = f"Alias '{alias}' does not match already configured connection '{database_uri}'"
            raise ValueError(msg)

        if alias_index is not None:
            return cls._clients[alias_index]

        if uri_index is not None:
            return cls._clients[uri_index]

        client = MongoClient(config)
        client_index = len(cls._clients)

        cls._clients.append(client)  # type: ignore
        cls._uris[database_uri] = client_index
        if alias is not None:
            cls._alias[alias] = client_index

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
