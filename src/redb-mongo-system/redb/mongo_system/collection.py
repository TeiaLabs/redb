from typing import Any, Type

from pymongo.collection import Collection as PymongoCollection

from redb.core import Document
from redb.interface.collection import Collection, Json, OptionalJson, ReturnType
from redb.interface.fields import CompoundIndex, Direction, PyMongoOperations
from redb.interface.results import (
    BulkWriteResult,
    DeleteManyResult,
    DeleteOneResult,
    InsertManyResult,
    InsertOneResult,
    ReplaceOneResult,
    UpdateManyResult,
    UpdateOneResult,
)


class MongoCollection(Collection):
    __client_name__: str = "mongo"

    def __init__(self, collection: PymongoCollection) -> None:
        super().__init__()

        self.__collection = collection

    def _get_driver_collection(self) -> PymongoCollection:
        return self.__collection

    def create_index(
        self,
        index: CompoundIndex,
    ) -> bool:
        name = index.name
        if name is None:
            name = "_".join([field.join_attrs("_") for field in index.fields])
            name = f"unique_{name}" if index.unique else name
            name = f"{index.direction.name.lower()}_{name}_index"
        try:
            keys = [(field.join_attrs(), index.direction) for field in index.fields]
            self.__collection.create_index(
                keys=keys,
                name=name,
                unique=index.unique,
            )
            return True
        except:
            return False

    def find(
        self,
        cls: Type[Document],
        return_cls: ReturnType,
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        sort: dict[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        return [
            return_cls(**result)
            for result in self.__collection.find(
                filter=filter,
                projection=fields,
                sort=sort,
                skip=skip,
                limit=limit,
            )
        ]

    def find_one(
        self,
        cls: Type[Document],
        return_cls: ReturnType,
        filter: OptionalJson = None,
        skip: int = 0,
    ) -> ReturnType:
        result = self.__collection.find_one(
            filter=filter,
            skip=skip,
        )
        return return_cls(**result)

    def distinct(
        self,
        cls: ReturnType,
        key: str,
        filter: OptionalJson = None,
    ) -> list[ReturnType]:
        results = self.__collection.distinct(key=key, filter=filter)
        return results

    def count_documents(
        self,
        cls: Type[Document],
        filter: OptionalJson = None,
    ) -> int:
        return self.__collection.count_documents(filter=filter)

    def bulk_write(
        self,
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        result = self.__collection.bulk_write(requests=operations)
        return BulkWriteResult(
            deleted_count=result.deleted_count,
            inserted_count=result.inserted_count,
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_count=result.upserted_count,
            upserted_ids=result.upserted_ids,
        )

    def insert_one(
        self,
        cls: Type[Document],
        data: dict,
    ) -> InsertOneResult:
        result = self.__collection.insert_one(document=data)
        return InsertOneResult(inserted_id=result.inserted_id)

    def insert_many(
        self,
        cls: Type[Document],
        data: list[Json],
    ) -> InsertManyResult:
        result = self.__collection.insert_many(documents=data)
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        self,
        cls: Type[Document],
        filter: Json,
        replacement: Json,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        result = self.__collection.replace_one(
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
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateOneResult:
        result = self.__collection.update_one(
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
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateManyResult:
        result = self.__collection.update_many(
            filter=filter,
            update=update,
            upsert=upsert,
        )
        return UpdateManyResult(
            matched_count=result.matched_count,
            modified_count=result.modified_count,
            upserted_id=result.upserted_id,
        )

    def delete_one(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteOneResult:
        self.__collection.delete_one(filter=filter)
        return DeleteOneResult()

    def delete_many(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteManyResult:
        result = self.__collection.delete_many(filter=filter)
        return DeleteManyResult(deleted_count=result.deleted_count)
