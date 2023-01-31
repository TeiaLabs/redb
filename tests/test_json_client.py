import json
from pathlib import Path

import pytest

from .utils import Embedding, read_json, remove_document


def test_insert_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert (collection_path / f"{d.id}.json").is_file()
    json_path = collection_path / f"{d.id}.json"
    other = read_json(json_path)
    remove_document(collection_path, d.id)
    assert other == d


def test_find_by_id(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        source_url="www",
    )
    d.insert_one()
    found_instance = Embedding.find_one(d)
    remove_document(collection_path, d.id)
    assert found_instance == d


def test_insert_vectors(collection_path: Path):
    data = dict(
        id=["a", "b", "c"],
        kb_name=["KB5", "KB6", "KB7"],
        model=["ai", "ai", "ai"],
        text=["Some data 5", "Some data 6", "Some data 7"],
        vector=[[1, 3], [2, 4], [3, 5]],
        source_url=["ww5", "ww6", "ww7"],
    )
    Embedding.insert_vectors(data)
    embeddings = [
        Embedding(**read_json(val)) for val in collection_path.glob("*.json")
    ]
    assert sorted(e.id for e in embeddings) == data["id"]
    embeddings.sort(key=lambda embedding: embedding.kb_name)
    for i in range(len(embeddings)):
        original = Embedding(
            kb_name=data["kb_name"][i],
            model=data["model"][i],
            text=data["text"][i],
            vector=data["vector"][i],
            source_url=data["source_url"][i],
        )
        other = embeddings[i]
        remove_document(collection_path, other.id)
        # The ids were swapped because the content changed, changing the hash, changing the id itself
        original.id = other.id
        # Make both timestamps be the same because they are being generated on object instantiation
        original.created_at = other.created_at
        original.updated_at = other.updated_at
        assert original == other


def test_count_documents(collection_path: Path):
    doc_count = Embedding.count_documents()
    assert doc_count == 0
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    doc_count = Embedding.count_documents()
    assert doc_count == 1
    remove_document(collection_path, d.id)


def test_replace_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    replacement = Embedding(
        kb_name="KB_REPLACED",
        model="replaced",
        text="Some replaced data.",
        vector=[114, 101, 112, 108, 97, 99, 101, 100],
        source_url="www.replaced",
    )
    d.replace_one(replacement)
    other = read_json(collection_path / f"{replacement.id}.json")
    remove_document(collection_path, replacement.id)
    assert replacement == other


def test_update_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    result = d.update_one(update={"kb_name": "KB_UPDATED"})
    d.id = result.upserted_id
    expected = d.dict()
    expected["kb_name"] = "KB_UPDATED"
    other = read_json(collection_path / f"{d.id}.json")
    remove_document(collection_path, d.id)
    assert expected == other


def test_delete_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    d.delete_one()
    with pytest.raises(AssertionError):
        assert (collection_path / f"{d.id}.json").is_file()
