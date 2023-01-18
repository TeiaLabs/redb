from typing import Any, TypeVar

from pymongo.collection import Collection as PymongoCollection

from redb.interface import (
    BulkWriteResult,
    Collection,
    CompoundIndice,
    DeleteManyResult,
    DeleteOneResult,
    Direction,
    IncludeColumn,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    ReplaceOneResult,
    SortColumn,
    UpdateManyResult,
    UpdateOneResult,
)

T = TypeVar("T", bound=dict[str, Any])


class MongoCollection(Collection):
    __client_name__: str = "mongo"

    def __init__(self, collection: PymongoCollection) -> None:
        super().__init__()

        self.collection = collection

    def _get_driver_collection(self) -> PymongoCollection:
        return self.collection

    def create_indice(self, indice: CompoundIndice) -> None:
        if indice.direction is None:
            indice.direction = Direction.ASCENDING

        name = indice.name
        if name is None:
            name = "_".join([field.get_joined_attrs("_") for field in indice.fields])
            name = f"unique_{name}" if indice.unique else name
            name = f"{indice.direction.name.lower()}_{name}"

        self.collection.create_index(
            [field.get_joined_attrs() for field in indice.fields],
            name=name,
            unique=indice.unique,
        )

    def find(
        self,
        filter: T | None = None,
        fields: list[IncludeColumn] | list[str] | None = None,
        sort: list[SortColumn] | SortColumn | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[T]:
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
            filter=filter,
            projection=formatted_fields,
            sort=formatted_sort,
            skip=skip,
            limit=limit,
        )

    def find_one(self, filter: T | None = None, skip: int = 0) -> dict:
        return self.collection.find_one(
            filter=filter,
            skip=skip,
        )

    def distinct(self, key: str, filter: T | None = None) -> list[dict]:
        return self.collection.distinct(
            key=key,
            filter=filter,
        )

    def count_documents(self, filter: T | None = None) -> int:
        return self.collection.count_documents(filter=filter)

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

    def insert_one(self, data: dict) -> InsertOneResult:
        result = self.collection.insert_one(document=data)
        return InsertOneResult(inserted_id=result.inserted_id)

    def insert_vectors(self, data: dict[str, list[Any]]) -> InsertManyResult:
        keys = list(data.keys())
        values_size = len(data[keys[0]])

        instances = [None] * values_size
        for i in range(values_size):
            instance = {}
            for key in keys:
                instance[key] = data[key][i]
            instances[i] = instance

        return self.insert_many(instances)

    def insert_many(self, data: list[T]) -> InsertManyResult:
        result = self.collection.insert_many(documents=data)
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        self,
        filter: T,
        replacement: T,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        result = self.collection.replace_one(
            filter=filter,
            replacement=replacement,
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
            filter=filter,
            update=update,
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
            filter=filter,
            update=update,
            upsert=upsert,
        )
        return UpdateManyResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def delete_one(self, filter: T) -> DeleteOneResult:
        self.collection.delete_one(filter=filter)
        return DeleteOneResult()

    def delete_many(self, filter: T) -> DeleteManyResult:
        result = self.collection.delete_many(filter=filter)
        return DeleteManyResult(deleted_count=result.deleted_count)
