import json
from pathlib import Path

from redb.core import Document


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    vector: list[float] = None
    source_url: str

    def __eq__(self, other: "Embedding") -> bool:
        if isinstance(other, Embedding):
            b = other.dict()
        elif isinstance(other, dict):
            b = other
        else:
            return False
        a = self.dict()
        a_keys = set(a.keys())
        b_keys = set(b.keys())
        assert a_keys == b_keys
        # Iterating is better than just "=="
        # in case of an error the log will be more readable :3
        for a_key in a_keys:
            assert a[a_key] == b[a_key]
        return True

class RussianDog(Document):
    name: str
    age: int
    breed: str
    color: str
    is_good_boy: bool

    def __eq__(self, other: "RussianDog") -> bool:
        if isinstance(other, RussianDog):
            b = other.dict()
        elif isinstance(other, dict):
            b = other
        else:
            return False
        a = self.dict()
        a_keys = set(a.keys())
        b_keys = set(b.keys())
        assert a_keys == b_keys
        # Iterating is better than just "=="
        # in case of an error the log will be more readable :3
        for a_key in a_keys:
            assert a[a_key] == b[a_key]
        return True


def remove_document(collection_path: Path, hash: str):
    json_path = collection_path / f"{hash}.json"
    json_path.unlink()


def read_json(path: Path):
    with open(path, "r") as f:
        return json.load(f)

