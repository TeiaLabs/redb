from dataclasses import dataclass, field
from typing import Any


@dataclass
class JSONConfig:
    client_folder_path: str
    default_database_folder_path: str | None = None


@dataclass
class MigoConfig:
    milvus_connection_alias: str
    milvus_host: str
    milvus_port: int
    mongo_database_uri: str
    mongo_kwargs: dict[str, Any] = field(default_factory=dict)
    milvus_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class MongoConfig:
    database_uri: str
    default_database: str | None = None
    driver_kwargs: dict = field(default_factory=dict)


CONFIGS = JSONConfig | MigoConfig | MongoConfig
CONFIG_TYPE = JSONConfig |  MigoConfig | MongoConfig | dict


def check_config(
    config: CONFIG_TYPE,
    base_class: CONFIGS,
) -> bool:
    return isinstance(config, base_class) or isinstance(config, dict)
