from typing import Any, Type, TypeVar

from pymongo.collection import Collection as PymongoCollection

from ..interfaces import (
    BulkWriteResult,
    Collection,
    DeleteManyResult,
    DeleteOneResult,
    Direction,
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


class MongoCollection(Collection):
    __client_name__: str = "mongo"

    def __new__(cls, collection_name: Collection | None = None, *_, **__):
        cls.__collection_name__ = collection_name or cls.__name__
        return super().__new__(cls)

    @classmethod
    def _get_driver_collection(cls: Type[T]):
        from ..instance import RedB

        client = RedB.get_client(cls.__client_name__)
        database = (
            client.get_database(cls.__database_name__)
            if cls.__database_name__
            else client.get_default_database()
        )
        collection_name = cls.__collection_name__ or cls.__name__
        return database._get_driver_database()[collection_name]

    @classmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        collection = cls._get_driver_collection()

        formatted_filter = filter
        if filter is not None:
            formatted_filter = filter.dict()

        formatted_fields = fields
        if fields is not None:
            if isinstance(fields[0], str):
                formatted_fields = {field: True for field in fields}
            else:
                formatted_fields = {field.name: field.include for field in fields}

        formatted_sort = sort
        if sort is not None:
            if isinstance(sort[0], str):
                formatted_sort = [(field, Direction.ASCENDING) for field in sort]
            else:
                formatted_sort = [(field.name, field.direction) for field in fields]

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
        collection = cls._get_driver_collection()

        formatted_filter = filter
        if filter is not None:
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
        collection = cls._get_driver_collection()

        formatted_filter = filter
        if filter is not None:
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
        collection = cls._get_driver_collection()

        formatted_filter = {}
        if filter is not None:
            formatted_filter = filter.dict()

        return collection.count_documents(filter=formatted_filter)

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = cls._get_driver_collection()

        result = collection.bulk_write(requests=operations)
        return BulkWriteResult(
            deleted_count=result.deleted_count,
            inserted_count=result.inserted_count,
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_count=result.upserted_count,
            upserted_ids=result.upserted_ids,
        )

    def insert_one(data: T) -> InsertOneResult:
        collection = data._get_driver_collection()

        result = collection.insert_one(document=data.dict())
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
        collection = cls._get_driver_collection()

        result = collection.insert_many(
            documents=[document.dict() for document in data]
        )
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        collection = filter._get_driver_collection()

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
        collection = filter._get_driver_collection()

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
        collection = cls._get_driver_collection()

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
        collection = filter._get_driver_collection()

        collection.delete_one(filter=filter.dict())
        return DeleteOneResult()

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        collection = cls._get_driver_collection()

        result = collection.delete_many(filter=filter.dict())
        return DeleteManyResult(deleted_count=result.deleted_count)
