import json
from pathlib import Path
from typing import Any, Type, TypeVar

from ..interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteManyResult,
    DeleteOneResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    ReplaceOneResult,
    SortField,
    UpdateManyResult,
    UpdateOneResult,
)

T = TypeVar("T", bound=Collection)


class JSONCollection(Collection):
    __client_name__ = "json"

    @classmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        collection_path = get_collection_path(cls)
        json_files = collection_path.glob("*.json")

        return [
            cls(**json.load(open(json_file)))
            for json_file in json_files
            if json_file.is_file()
        ]

    @classmethod
    def find_vectors(
        cls: Type[T],
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    @classmethod
    def find_one(
        cls: Type[T],
        filter: T | None = None,
        skip: int = 0,
    ) -> T:
        pass

    @classmethod
    def find_by_id(cls: Type[T], id: str) -> T | None:
        collection_path = get_collection_path(cls)
        json_path = collection_path / Path(f"{id}.json")
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            return cls(**data)
        return None

    @classmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        pass

    @classmethod
    def count_documents(
        cls: Type[T],
        filter: T | None = None,
    ) -> int:
        collection_path = get_collection_path(cls)
        json_files = collection_path.glob("*.json")
        return len(list(json_files))

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    def insert_one(data: T) -> InsertOneResult:
        collection_path = get_collection_path(data.__class__)
        collection_path.mkdir(parents=True, exist_ok=True)

        id = data.get_hash()
        json_path = collection_path / Path(f"{id}.json")
        if json_path.is_file():
            raise ValueError(f"Document with {id} already exists!")

        data = data.dict()
        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)

        return InsertOneResult(inserted_id=id)

    @classmethod
    def insert_vectors(
        cls: Type[T],
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        size = len(data[next(iter(data.keys()))])
        ids = []
        for i in range(size):
            obj = {k: data[k][i] for k in data}
            ids.append(cls(**obj).insert_one().inserted_id)

        return InsertManyResult(inserted_ids=ids)

    @classmethod
    def insert_many(
        cls: Type[T],
        data: list[T],
    ) -> InsertManyResult:
        ids = []
        for item in data:
            if isinstance(item, dict):
                item = cls(**item)
            if isinstance(item, cls):
                ids.append(item.insert_one().inserted_id)
            else:
                raise ValueError(
                    f"Document '{item}' could not be converted to class '{cls}'"
                )

        return InsertManyResult(inserted_ids=ids)

    def replace_one(
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        collection_path = get_collection_path(filter.__class__)
        collection_path.mkdir(parents=True, exist_ok=True)

        upserted = False
        id = filter.get_hash()
        json_path = collection_path / Path(f"{id}.json")
        data = replacement.dict()
        if json_path.is_file():
            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)
        elif upsert:
            upserted = True
            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)
        else:
            raise ValueError(f"Document with {id} does not exists!")

        return ReplaceOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=id if upserted else None,
        )

    def update_one(
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        collection_path = get_collection_path(filter.__class__)
        collection_path.mkdir(parents=True, exist_ok=True)

        upserted = False
        id = filter.get_hash()
        json_path = collection_path / Path(f"{id}.json")
        obj = update.dict(exclude_unset=True)
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)

            data.update(obj)
            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)

        elif upsert:
            upserted = True
            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)
        else:
            raise ValueError(f"Document with {id} does not exists!")

        return UpdateOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=id if upserted else None,
        )

    @classmethod
    def update_many(
        cls: Type[T],
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        upserted_ids = []

        documents = filter.find()
        for document in documents:
            result = document.update_one(document, update, upsert)
            upserted_ids.append(result.upserted_id)

        return UpdateManyResult(
            matched_count=len(filter),
            modified_count=len(upserted_ids),
            upserted_id=upserted_ids,
        )

    def delete_one(filter: T) -> DeleteOneResult:
        collection_path = get_collection_path(filter.__class__)
        id = filter.get_hash()
        file = collection_path / f"{id}.json"
        if not file.exists():
            raise ValueError(f"Document with {id} does not exists!")

        file.unlink()
        return DeleteOneResult()

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        documents = filter.find()
        for document in documents:
            document.delete_one()

        return DeleteManyResult(deleted_count=len(documents))


def get_collection_path(cls: Type[JSONCollection]) -> Path:
    from ..instance import RedB

    client = RedB.get_client(cls.__client_name__)
    database = (
        client.get_database(cls.__database_name__)
        if cls.__database_name__
        else client.get_default_database()
    )
    return client.client_path / database.database_path / cls.__name__