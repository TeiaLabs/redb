import json
from pathlib import Path

import pytest

from .utils import Embedding, assert_exists, compare_values, remove_document


def test_insert_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    json_path = collection_path / f"{d.get_hash()}.json"
    other = json.load(open(json_path))

    remove_document(collection_path, d.get_hash())
    compare_values(other, d)


def test_find_by_id(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    found_instance = Embedding.find_by_id(d.get_hash())

    remove_document(collection_path, d.get_hash())
    compare_values(found_instance, d)


def test_insert_vectors(collection_path: Path):
    data = dict(
        kb_name=["KB5", "KB6", "KB7"],
        model=["ai", "ai", "ai"],
        text=["Some data 5", "Some data 6", "Some data 7"],
        vector=[[1, 3], [2, 4], [3, 5]],
        source_url=["ww5", "ww6", "ww7"],
    )
    Embedding.insert_vectors(data)

    embeddings = [
        Embedding(**json.load(open(val))) for val in collection_path.glob("*.json")
    ]
    assert len(embeddings) == 3

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
        remove_document(collection_path, other.get_hash())
        compare_values(original, other)


def test_count_documents(collection_path: Path):
    assert Embedding.count_documents() == 0

    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    assert Embedding.count_documents() == 1

    remove_document(collection_path, d.get_hash())


def test_replace_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    replacement = Embedding(
        kb_name="KB_REPLACED",
        model="replaced",
        text="Some replaced data.",
        vector=[114, 101, 112, 108, 97, 99, 101, 100],
        source_url="www.replaced",
    )

    d.replace_one(replacement)

    other = json.load(open(collection_path / f"{d.get_hash()}.json"))

    remove_document(collection_path, d.get_hash())
    compare_values(replacement, other)


def test_update_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    update = Embedding.construct(kb_name="KB_UPDATED")
    d.update_one(update)

    expected = d.dict()
    expected["kb_name"] = "KB_UPDATED"
    other = json.load(open(collection_path / f"{d.get_hash()}.json"))

    remove_document(collection_path, d.get_hash())
    print(expected, other)
    compare_values(expected, other)


def test_delete_one(collection_path: Path):
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert_one()
    assert_exists(collection_path, d.get_hash())

    d.delete_one()

    with pytest.raises(AssertionError):
        assert_exists(collection_path, d.get_hash())
