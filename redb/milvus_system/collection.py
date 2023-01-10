import json
import time
from pathlib import Path
from typing import Any, Literal, Type, TypeVar

import numpy as np
from pymilvus import Collection, CollectionSchema, DataType, FieldSchema

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

T = TypeVar("T")


class MilvusCollection(Collection):
    __schema__ = None
    __dtype_map__ = {
        # NONE = 0
        # BOOL = 1
        # INT8 = 2
        # INT16 = 3
        # INT32 = 4
        # INT64 = 5
        # FLOAT = 10
        # DOUBLE = 11
        # STRING = 20
        # VARCHAR = 21
        # BINARY_VECTOR = 100
        # FLOAT_VECTOR = 101
        # UNKNOWN = 999
        "int": DataType.INT64,
        "float": DataType.FLOAT,
        "str": DataType.VARCHAR,
        "bool": DataType.BOOL,
        "datetime": DataType.VARCHAR,
        "date": DataType.VARCHAR,
        "time": DataType.VARCHAR,
        "object": DataType.VARCHAR,
        "category": DataType.VARCHAR,
        "bytes": DataType.VARCHAR,
        "decimal": DataType.VARCHAR,
        "timedelta": DataType.VARCHAR,
        "complex": DataType.VARCHAR,
        "uint": DataType.VARCHAR,
        "list[float]": DataType.FLOAT_VECTOR,
        "list[int]": DataType.BINARY_VECTOR,
    }

    def __new__(cls, *args, **kwargs):
        """Flyweight pattern for the Milvus collection schema."""
        if cls.__schema__ is not None:
            return super().__new__(cls)
        print("MilvusCollection", args, kwargs)
        fields = []
        for attr in cls.__fields__.values():
            # TODO:
            # max_length for VARCHAR
            # dim for
            fields.append(
                FieldSchema(
                    name=attr.name,
                    dtype=cls.__dtype_map__[attr.type_.__name__],
                    is_primary=False,
                    auto_id=False,
                    max_length=100,
                )
            )
        fields.append(
            FieldSchema(
                name="_id",
                dtype=DataType.INT64,
                is_primary=True,
                auto_id=False,
            )
        )
        cls.__schema__ = CollectionSchema(fields, cls.__doc__)
        return super().__new__(cls)

    @staticmethod
    def _get_driver_collection(instance_or_class: Type[T] | T) -> "Collection":
        if isinstance(instance_or_class, type):
            collection_name = instance_or_class.__name__
        else:
            collection_name = (
                object.__getattribute__(instance_or_class, "__collection_name__")
                or instance_or_class.__class__.__name__
            )

        return get_collection_path(
            instance_or_class.__client_name__,
            collection_name,
            instance_or_class.__database_name__,
        )

    @classmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeDBColumn] | list[str] | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        collection_path = JSONCollection._get_driver_collection(cls)
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
        sort: list[SortDBColumn] | SortDBColumn | None = None,
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
        collection_path = JSONCollection._get_driver_collection(cls)
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
        collection_path = JSONCollection._get_driver_collection(cls)
        json_files = collection_path.glob("*.json")
        return len(list(json_files))

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        pass

    def insert_one(data: T) -> InsertOneResult:
        collection_path = JSONCollection._get_driver_collection(data)
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
        collection_path = JSONCollection._get_driver_collection(filter)
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
        collection_path = JSONCollection._get_driver_collection(filter)
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
        collection_path = JSONCollection._get_driver_collection(filter)
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
        pass
