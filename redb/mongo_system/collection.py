from typing import Any, Type, TypeVar

from pymongo.collection import Collection as PymongoCollection

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
from ..interfaces.fields import CompoundIndex, Direction

T = TypeVar("T", bound=Document)


class MongoCollection(Collection):
    __client_name__: str = "mongo"

    def __init__(self, collection: PymongoCollection) -> None:
        super().__init__()

        self.collection = collection

    def _get_driver_collection(self):
        return self.collection

    def create_indice(self, indice: CompoundIndex) -> None:
        if indice.direction is None:
            indice.direction = Direction.ASCENDING

        name = indice.name
        if name is None:
            name = "_".join([
                name.alias if hasattr(name, 'alias') else "id"
                for name in indice.fields
            ])
            name = f"unique_{name}" if indice.unique else name
            name = f"{indice.direction.name.lower()}_{name}"

        self.collection.create_index([
                (name.alias, indice.direction.value)
                if hasattr(name, 'alias')
                else ("id", indice.direction.value)
                for name in indice.fields
            ],
            name=name,
            unique=indice.unique,
        )

    def find(
        self,
        filter: T | None = None,
        fields: list[IncludeDBColumn] | list[str] | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[T]:
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

        return self.collection.find(
            filter=formatted_filter,
            projection=formatted_fields,
            sort=formatted_sort,
            skip=skip,
            limit=limit,
        )

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
        formatted_filter = filter
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return self.collection.find_one(
            filter=formatted_filter,
            skip=skip,
        )

    def distinct(
        self,
        key: str,
        filter: T | None = None,
    ) -> list[T]:
        formatted_filter = filter
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return self.collection.distinct(
            key=key,
            filter=formatted_filter,
        )

    def count_documents(
        self,
        filter: T | None = None,
    ) -> int:
        formatted_filter = {}
        if filter is not None and not isinstance(filter, dict):
            formatted_filter = filter.dict()

        return self.collection.count_documents(filter=formatted_filter)

    def bulk_write(
        self,
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        result = self.collection.bulk_write(requests=operations)
        return BulkWriteResult(
            deleted_count=result.deleted_count,
            inserted_count=result.inserted_count,
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_count=result.upserted_count,
            upserted_ids=result.upserted_ids,
        )

    def insert_one(self, data: T) -> InsertOneResult:
        result = self.collection.insert_one(document=data.dict())
        return InsertOneResult(inserted_id=result.inserted_id)

    def insert_vectors(
        self,
        data: dict[str, list[Any]],
    ) -> InsertManyResult:
        pass

    def insert_many(
        self,
        data: list[T],
    ) -> InsertManyResult:
        result = self.collection.insert_many(
            documents=[document.dict() for document in data]
        )
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        self,
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        result = self.collection.replace_one(
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
        self,
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateOneResult:
        result = self.collection.update_one(
            filter=filter.dict(),
            update=update.dict(),
            upsert=upsert,
        )
        return UpdateOneResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def update_many(
        self,
        filter: T,
        update: T,
        upsert: bool = False,
    ) -> UpdateManyResult:
        result = self.collection.update_many(
            filter=filter.dict(),
            update=update.dict(),
            upsert=upsert,
        )
        return UpdateManyResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def delete_one(self, filter: T) -> DeleteOneResult:
        self.collection.delete_one(filter=filter.dict())
        return DeleteOneResult()

    def delete_many(
        self,
        filter: T,
    ) -> DeleteManyResult:
        result = self.collection.delete_many(filter=filter.dict())
        return DeleteManyResult(deleted_count=result.deleted_count)
