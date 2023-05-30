import json
from pathlib import Path

import pytest

from .utils import Embedding, read_json, remove_document


@pytest.fixture
def embedding():
    return Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )


def test_insert_one(json_client, collection_path: Path, embedding: Embedding):
    # Ignored while we do not have a Mongo instance to run agains
    # with transaction(collection_class=Embedding, config=MongoConfig(database_uri="mongodb://localhost:27017/", default_database="teia"), database_name="another") as embedding:
    #     embedding.insert_one(data=d)
    #     assert not (collection_path / f"{d.id}.json").is_file()
    Embedding.insert_one(embedding)
    assert (collection_path / f"{embedding.id}.json").is_file()
    json_path = collection_path / f"{embedding.id}.json"
    other = read_json(json_path)
    remove_document(collection_path, embedding.id)
    assert other == embedding


def test_find_by_id(json_client, collection_path: Path, embedding: Embedding):
    Embedding.insert_one(embedding)
    found_instance = Embedding.find_one(filter=dict(_id=embedding.id))
    remove_document(collection_path, embedding.id)
    assert found_instance == embedding


def test_insert_vectors(json_client, collection_path: Path):
    data = dict(
        _id=["a", "b", "c"],
        kb_name=["KB5", "KB6", "KB7"],
        model=["ai", "ai", "ai"],
        text=["Some data 5", "Some data 6", "Some data 7"],
        vector=[[1, 3], [2, 4], [3, 5]],
        source_url=["ww5", "ww6", "ww7"],
    )
    Embedding.insert_vectors(data)
    embeddings = [Embedding(**read_json(val)) for val in collection_path.glob("*.json")]
    assert sorted(e.id for e in embeddings) == data["_id"]
    embeddings.sort(key=lambda embedding: embedding.kb_name)
    for i in range(len(embeddings)):
        original = Embedding(
            id=data["_id"][i],
            kb_name=data["kb_name"][i],
            model=data["model"][i],
            text=data["text"][i],
            vector=data["vector"][i],
            source_url=data["source_url"][i],
        )
        other = embeddings[i]
        remove_document(collection_path, other.id)
        # Make both timestamps be the same because they are being generated on object instantiation
        original.created_at = other.created_at
        original.updated_at = other.updated_at
        assert original == other


def test_count_documents(json_client, collection_path: Path, embedding: Embedding):
    doc_count = Embedding.count_documents()
    assert doc_count == 0
    Embedding.insert_one(embedding)
    doc_count = Embedding.count_documents()
    try:
        assert doc_count == 1
    finally:
        remove_document(collection_path, embedding.id)


def test_replace_one(json_client, collection_path: Path, embedding: Embedding):
    Embedding.insert_one(embedding)
    replacement = Embedding(
        kb_name="KB_REPLACED",
        model="replaced",
        text="Some replaced data.",
        vector=[114, 101, 112, 108, 97, 99, 101, 100],
        source_url="www.replaced",
    )
    Embedding.replace_one(embedding, replacement)
    other = read_json(collection_path / f"{replacement.id}.json")
    remove_document(collection_path, replacement.id)
    assert replacement == other


def test_update_one(json_client, collection_path: Path, embedding: Embedding):
    Embedding.insert_one(embedding)
    Embedding.update_one(embedding, update={"vector": [1, 2, 3]})
    expected = embedding.dict()
    expected["vector"] = [1, 2, 3]
    other = read_json(collection_path / f"{embedding.id}.json")
    remove_document(collection_path, embedding.id)
    for key, value in expected.items():
        if key == "updated_at":
            continue

        assert key in other
        assert value == other[key]


def test_delete_one(json_client, collection_path: Path, embedding: Embedding):
    try:
        Embedding.insert_one(embedding)
    except:
        pass
    Embedding.delete_one(embedding)
    with pytest.raises(AssertionError):
        assert (collection_path / f"{embedding.id}.json").is_file()
