from pathlib import Path

from redb.core import Document


class Embedding(Document):
    kb_name: str
    model: str
    text: str
    vector: list[float] = None
    source_url: str


def compare_values(a: dict | Embedding, b: dict | Embedding):
    if isinstance(a, Embedding):
        a = a.dict()
    if isinstance(b, Embedding):
        b = b.dict()

    a_keys = set(a.keys())
    b_keys = set(b.keys())

    assert a_keys == b_keys

    # Iterating is better than just "==" because
    # in case of an error the log will be more readable
    for a_key in a_keys:
        assert a[a_key] == b[a_key]


def remove_document(collection_path: Path, hash: str):
    json_path = collection_path / f"{hash}.json"
    json_path.unlink()


def assert_exists(collection_path: Path, hash: str):
    json_path = collection_path / f"{hash}.json"
    assert json_path.is_file()
