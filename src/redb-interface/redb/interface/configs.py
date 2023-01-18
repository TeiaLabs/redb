from dataclasses import dataclass, field


@dataclass
class JSONConfig:
    client_folder_path: str
    default_database_folder_path: str | None = None


@dataclass
class MigoConfig:
    client_folder_path: str


@dataclass
class MongoConfig:
    database_uri: str
    default_database: str | None = None
    driver_kwargs: dict = field(default_factory=dict)
