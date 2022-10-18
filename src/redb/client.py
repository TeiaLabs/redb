from pathlib import Path
from typing import Any, Type


class Client:

    attrs: dict[str, Any]

    def __init__(self, **kwargs) -> None:
        print("Initialized DB client.")


class JSONClient(Client):
    def __init__(self, **kwargs):
        self.attrs = kwargs
        file_path: Path = kwargs["file_path"]
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch(exist_ok=True)
        super().__init__()


class MongoClient(Client):
    def __init__(self, auth: str):
        self.auth = auth
        super().__init__()
