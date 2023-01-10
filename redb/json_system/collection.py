import json
from pathlib import Path
from typing import Any, Dict, Type, TypeVar

from bson import ObjectId

from ..document import Document
from ..interfaces import (
    BulkWriteResult,
    Collection,
    DeleteManyResult,
    DeleteOneResult,
    IncludeDBColumn,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    ReplaceOneResult,
    SortDBColumn,
    UpdateManyResult,
    UpdateOneResult,
)
from ..interfaces.fields import CompoundIndex

T = TypeVar("T", bound=Document)


class JSONCollection(Collection):
    __client_name__ = "json"

    def __init__(self, collection: Path, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.collection = collection

    def _get_driver_collection(self):
        return self.collecton

    def create_indice(self, _: CompoundIndex) -> None:
        pass

    def find(
        self,
        filter: T | None = None,
        fields: list[IncludeDBColumn] | list[str] | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        json_files = self.collection.glob("*.json")

        transform = lambda file_path: json.load(open(file_path))

        out = []
        for json_file in json_files:
            if json_file.is_file():
                out.append(transform(json_file))

        return out

    def find_vectors(
        self,
        column: str | None = None,
        filter: T | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        pass

    def find_one(
        self,
        filter: T | None = None,
        skip: int = 0,
    ) -> T:
        pass

    def find_by_id(self, id: str) -> T | None:
        json_path = self.collection / Path(f"{id}.json")
        if json_path.is_file():
            with open(json_path) as f:
                data = json.load(f)
            return data
        return None

    def distinct(
        self,
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        pass

    def count_documents(
        self,
        filter: T | None = None,
    ) -> int:
        json_files = self.collection.glob("*.json")
        return len(list(json_files))

    def bulk_write(
        self,
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    def insert_one(self, data: T) -> InsertOneResult:
        self.collection.mkdir(parents=True, exist_ok=True)

        if not isinstance(data, dict):
            data = data.dict()

        id = data["id"]
        json_path = self.collection / Path(f"{id}.json")
        if json_path.is_file():
            raise ValueError(f"Document with {id} already exists!")

        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)

        return InsertOneResult(inserted_id=id)

    def insert_vectors(
        self,
        cls: Type[T],
        data: Dict[str, list[Any]],
    ) -> InsertManyResult:
        size = len(data[next(iter(data.keys()))])
        ids = []
        for i in range(size):
            obj = {k: data[k][i] for k in data}
            ids.append(self.insert_one(cls(**obj)).inserted_id)

        return InsertManyResult(inserted_ids=ids)

    def insert_many(
        self,
        data: list[T],
    ) -> InsertManyResult:
        ids = []
        for item in data:
            ids.append(self.insert_one(item).inserted_id)

        return InsertManyResult(inserted_ids=ids)

    def replace_one(
        self,
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        self.collection.mkdir(parents=True, exist_ok=True)

        upserted = False
        id = filter.get_hash()
        json_path = self.collection / Path(f"{id}.json")
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
        self,
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        self.collection.mkdir(parents=True, exist_ok=True)

        upserted = False
        id = filter.get_hash()
        json_path = self.collection / Path(f"{id}.json")
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

    def update_many(
        self,
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

    def delete_one(self, filter: T) -> DeleteOneResult:
        id = filter.get_hash()
        file = self.collection / f"{id}.json"
        if not file.exists():
            raise ValueError(f"Document with {id} does not exists!")

        file.unlink()
        return DeleteOneResult()

    def delete_many(
        self,
        filter: T,
    ) -> DeleteManyResult:
        documents = filter.find()
        for document in documents:
            document.delete_one()

        return DeleteManyResult(deleted_count=len(documents))
