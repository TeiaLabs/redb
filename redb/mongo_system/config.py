from dataclasses import dataclass, field


@dataclass
class MongoConfig:
    database_uri: str
    default_database: str | None = None
    driver_kwargs: dict = field(default_factory=dict)
