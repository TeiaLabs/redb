from typing import Any

from pymongo.collection import Collection as PymongoCollection

from redb.interface import (
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
from redb.interface.fields import CompoundIndex, Direction


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
            name = "_".join(
                [
                    name.alias if hasattr(name, "alias") else "id"
                    for name in indice.fields
                ]
            )
            name = f"unique_{name}" if indice.unique else name
            name = f"{indice.direction.name.lower()}_{name}"

        self.collection.create_index(
            [
                (name.alias, indice.direction.value)
                if hasattr(name, "alias")
                else ("id", indice.direction.value)
                for name in indice.fields
            ],
            name=name,
            unique=indice.unique,
        )

    def find(
        self,
        filter: dict | None = None,
        fields: list[IncludeDBColumn] | list[str] | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[dict]:
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

    def find_vectors(
        self,
        column: str | None = None,
        filter: dict | None = None,
        sort: list[SortDBColumn] | SortDBColumn | None = None,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[dict]:
        pass

    def find_one(self, filter: dict | None = None, skip: int = 0) -> dict:
        return self.collection.find_one(
            filter=filter,
            skip=skip,
        )

    def distinct(self, key: str, filter: dict | None = None) -> list[dict]:
        return self.collection.distinct(
            key=key,
            filter=filter,
        )

    def count_documents(self, filter: dict | None = None) -> int:
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
        pass

    def insert_many(self, data: list[dict]) -> InsertManyResult:
        result = self.collection.insert_many(documents=data)
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        self,
        filter: dict,
        replacement: dict,
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
        filter: dict,
        update: dict,
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
        filter: dict,
        update: dict,
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

    def delete_one(self, filter: dict) -> DeleteOneResult:
        self.collection.delete_one(filter=filter)
        return DeleteOneResult()

    def delete_many(self, filter: dict) -> DeleteManyResult:
        result = self.collection.delete_many(filter=filter)
        return DeleteManyResult(deleted_count=result.deleted_count)
