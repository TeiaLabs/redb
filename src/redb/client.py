from pathlib import Path


class Client():

    @classmethod
    def from_prefix(cls, prefix: str, **kwargs):
        client_class: Client
        for subclass in cls.__subclasses__:
            if subclass.__name__.startswith(prefix):
                client_class = subclass
                break
        else:
            raise ValueError(f"Client with prefix {prefix} not found.")
        return client_class(**kwargs)


class JSONClient():

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.touch(exist_ok=True)
