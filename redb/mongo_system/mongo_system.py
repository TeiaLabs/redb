from typing import Any, Type, TypeVar

from pymongo import MongoClient as PymongoClient
from pymongo.collection import Collection as PymongoCollection
from pymongo.database import Database as PymongoDatabase

from ..interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
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


class MongoClient(Client):
    def __init__(
        self, db_uri: str, default_database: str | None = None, **kwargs
    ) -> None:
        self.client = PymongoClient(db_uri, **kwargs)
        if default_database is None:
            self.default_database = MongoDatabase(self.client.get_default_database())
        else:
            self.default_database = self.get_database(default_database)

    def get_databases(self) -> list[Database]:
        return [MongoDatabase(database) for database in self.client.list_databases()]

    def get_database(self, name: str) -> Database:
        return MongoDatabase(self.client.get_database(name))

    def get_default_database(self) -> Database:
        return self.default_database

    def drop_database(self, name: str) -> None:
        self.client.drop_database(name)

    def close(self) -> None:
        self.client.close()


class MongoDatabase(Database):
    def __init__(self, database: PymongoDatabase) -> None:
        self.database = database

    def get_collections(self) -> list[Collection]:
        return [
            MongoCollection(collection)
            for collection in self.database.list_collections()
        ]

    def get_collection(self, name: str) -> Collection:
        return MongoCollection(self.database[name])

    def create_collection(self, name: str) -> None:
        self.database.create_collection(name)

    def delete_collection(self, name: str) -> None:
        self.database.drop_collection(name)

    def __getitem__(self, name) -> Collection:
        return MongoCollection(self.database[name])


class MongoCollection(Collection):
    __client_name__ = "mongo"

    @classmethod
    def find(
        cls: Type[T],
        filter: T | None = None,
        fields: list[IncludeField] | list[str] | None = None,
        sort: list[SortField] | SortField | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
        collection = get_pymongo_collection(cls)

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
        collection = get_pymongo_collection(cls)

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
        collection = get_pymongo_collection(cls)

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
        collection = get_pymongo_collection(cls)

        formatted_filter = {}
        if filter is not None:
            formatted_filter = filter.dict()

        return collection.count_documents(filter=formatted_filter)

    @classmethod
    def bulk_write(
        cls: Type[T],
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        collection = get_pymongo_collection(cls)

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
        collection = get_pymongo_collection(data.__class__)

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
        collection = get_pymongo_collection(cls)

        result = collection.insert_many(
            documents=[document.dict() for document in data]
        )
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        collection = get_pymongo_collection(filter.__class__)

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
        collection = get_pymongo_collection(filter.__class__)

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
        collection = get_pymongo_collection(cls)

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
        collection = get_pymongo_collection(filter.__class__)

        collection.delete_one(filter=filter.dict())
        return DeleteOneResult()

    @classmethod
    def delete_many(
        cls: Type[T],
        filter: T,
    ) -> DeleteManyResult:
        collection = get_pymongo_collection(cls)

        result = collection.delete_many(filter=filter.dict())
        return DeleteManyResult(deleted_count=result.deleted_count)


def get_pymongo_collection(cls: Type[MongoCollection]) -> PymongoCollection:
    from ..instance import RedB

    client = RedB.get_client(cls.__client_name__)
    database = (
        client.get_database(cls.__database_name__)
        if cls.__database_name__
        else client.get_default_database()
    )

    return database[cls.__name__]
