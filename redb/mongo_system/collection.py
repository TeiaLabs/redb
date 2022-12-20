from typing import Any, Type, TypeVar

from pymongo.collection import Collection as PymongoCollection

from ..base import BaseCollection as Collection
from ..interfaces import (
    BulkWriteResult,
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
from ..interfaces.fields import Index

T = TypeVar("T", bound=Collection)


class MongoCollection(Collection):
    __client_name__: str = "mongo"

    @staticmethod
    def _get_driver_collection(instance_or_class: Type[T] | T) -> "Collection":
        if isinstance(instance_or_class, type):
            collection_name = instance_or_class.__name__
        else:
            collection_name = (
                getattr(instance_or_class, "__collection_name__", None)  # TODO: get this from somewhere else
                or instance_or_class.__class__.__name__
            )

        return get_pymongo_collection(
            instance_or_class.__client_name__,
            collection_name,
            instance_or_class.__database_name__,
        )

    @classmethod
    def create_indice(cls: Type[T], indice: Index) -> None:
        collection = MongoCollection._get_driver_collection(cls)

        collection.create_index(
            [
                (name, direction.value)
                for name, direction in zip(indice.names[1:], indice.directions)
            ],
            name="_".join(indice.names),
            unique=indice.unique,
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
        collection = MongoCollection._get_driver_collection(cls)

        formatted_filter = filter
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        formatted_fields = fields
        if fields is not None:
            if isinstance(fields[0], str):
                formatted_fields = {field: True for field in fields}
            else:
                formatted_fields = {field.name: field.include for field in fields}

        formatted_sort = sort
        if sort is not None:
            if isinstance(sort, list):
                formatted_sort = [(field.name, field.direction) for field in sort]
            else:
                formatted_sort = [(sort.name, sort.direction)]

        return [
            cls(**result)
            for result in collection.find(
                filter=formatted_filter,
                projection=formatted_fields,
                sort=formatted_sort,
                skip=skip,
                limit=limit,
            )
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
        collection = MongoCollection._get_driver_collection(cls)

        formatted_filter = filter
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return cls(
            **collection.find_one(
                filter=formatted_filter,
                skip=skip,
            )
        )

    @classmethod
    def distinct(
        cls: Type[T],
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        collection = MongoCollection._get_driver_collection(cls)

        formatted_filter = filter
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return [
            cls(**result)
            for result in collection.distinct(
                key=key,
                filter=formatted_filter,
            )
        ]

    @classmethod
    def count_documents(
        cls: Type[T],
        filter: T | None = None,
    ) -> int:
        collection = MongoCollection._get_driver_collection(cls)

        formatted_filter = {}
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return collection.count_documents(filter=formatted_filter)

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = MongoCollection._get_driver_collection(cls)

        result = collection.bulk_write(requests=operations)
        return BulkWriteResult(
            deleted_count=result.deleted_count,
            inserted_count=result.inserted_count,
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_count=result.upserted_count,
            upserted_ids=result.upserted_ids,
        )

    def insert_one(self: T) -> InsertOneResult:
        collection = MongoCollection._get_driver_collection(self)
        result = collection.insert_one(document=self.dict())
        return InsertOneResult(inserted_id=result.inserted_id)

    @classmethod
    def insert_vectors(
        cls: Type[T],
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        pass

    @classmethod
    def insert_many(
        cls: Type[T],
        data: list[T],
    ) -> InsertManyResult:
        collection = MongoCollection._get_driver_collection(cls)

        result = collection.insert_many(
            documents=[document.dict() for document in data]
        )
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        collection = MongoCollection._get_driver_collection(filter)

        result = collection.replace_one(
            filter=filter.dict(),
            replacement=replacement.dict(),
            upsert=upsert,
        )
        return ReplaceOneResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def update_one(
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        collection = MongoCollection._get_driver_collection(filter)

        result = collection.update_one(
            filter=filter.dict(),
            update=update.dict(),
            upsert=upsert,
        )
        return UpdateOneResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    @classmethod
    def update_many(
        cls: Type[T],
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        collection = MongoCollection._get_driver_collection(cls)

        result = collection.update_many(
            filter=filter.dict(),
            update=update.dict(),
            upsert=upsert,
        )
        return UpdateManyResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def delete_one(filter: T) -> DeleteOneResult:
        collection = MongoCollection._get_driver_collection(filter)

        collection.delete_one(filter=filter.dict())
        return DeleteOneResult()

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        collection = MongoCollection._get_driver_collection(cls)

        result = collection.delete_many(filter=filter.dict())
        return DeleteManyResult(deleted_count=result.deleted_count)


def get_pymongo_collection(
    client_name: str,
    collection_name: str,
    database_name: str | None = None,
) -> PymongoCollection:
    from ..instance import RedB

    client = RedB.get_client(client_name)
    database = (
        client.get_database(database_name)
        if database_name
        else client.get_default_database()
    )
    return database._get_driver_database()[collection_name]
