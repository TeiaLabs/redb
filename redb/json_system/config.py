from dataclasses import dataclass, field


@dataclass
class JSONConfig:
    client_folder_path: str
    default_database_folder_path: str | None = None
