from functools import lru_cache
from typing import Type, TypeVar

from migo.collection import BatchDocument
from migo.collection import Collection as MigoDriverCollection
from migo.collection import Document as MigoDocument
from migo.collection import Field as MigoField
from migo.collection import Filter as MigoFilter

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

T = TypeVar("T", bound=Type[MigoDocument | MigoFilter])


@lru_cache(maxsize=128)
def _build_migo_data(
    cls: Type[Document],
    data: OptionalJson,
    out: T,
) -> T | None:
    if data is None:
        return None

    mongo_data = {}
    milvus_data = {}
    for name, field in cls.__fields__.items():
        if name in data:
            if hasattr(field, "vector_type") and field.vector_type is not None:
                milvus_data[name] = data[name]
            else:
                mongo_data[name] = data[name]

    if not mongo_data:
        mongo_data = None

    if not milvus_data:
        milvus_data = None

    return out(mongo_data, milvus_data)


@lru_cache(maxsize=128)
def _build_migo_fields(
    cls: Type[Document],
    fields: dict[str, bool] | None,
) -> list[MigoField] | None:
    if fields is None:
        return None

    migo_fields = []
    for name, field in cls.__fields__.items():
        if name in fields:
            if hasattr(field, "vector_type") and field.vector_type is not None:
                migo_fields.append(MigoField(milvus_field=name))
            else:
                migo_fields.append(MigoField(mongo_field=name))

    return migo_fields


class MigoCollection(Collection):
    __client_name__: str = "migo"

    def __init__(self, collection: MigoDriverCollection) -> None:
        super().__init__()

        self.__collection = collection

    def _get_driver_collection(self) -> MigoDriverCollection:
        return self.__collection

    def create_index(
        self,
        index: CompoundIndex,
    ) -> bool:
        name = index.name
        if name is None:
            name = "_".join([field.join_attrs("_") for field in index.fields])
            name = f"unique_{name}" if index.unique else name
            name = f"{index.direction.name.lower()}_{name}"

        try:
            self.__collection.create_index(
                [field.join_attrs() for field in index.fields],
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
        _: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
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

        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        migo_fields = _build_migo_fields(cls, formatted_fields)
        results = self.__collection.find_many(
            filter=migo_filter,
            sort=formatted_sort,
            fields=migo_fields,
            limit=limit,
        )
        return [return_cls(**result) for result in results]

    def find_one(
        self,
        cls: Type[Document],
        return_cls: ReturnType,
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        _: int = 0,
    ) -> ReturnType:
        formatted_fields = fields
        if fields is not None:
            if isinstance(fields[0], str):
                formatted_fields = {field: True for field in fields}
            else:
                formatted_fields = {field.name: field.include for field in fields}

        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        migo_fields = _build_migo_fields(cls, formatted_fields)
        results = self.__collection.find_one(filter=migo_filter, fields=migo_fields)
        return [return_cls(**result) for result in results]

    def distinct(
        self,
        cls: ReturnType,
        key: str,
        filter: OptionalJson = None,
    ) -> list[ReturnType]:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        result = self.__collection.distinct(key=key, filter=migo_filter)
        return cls(**result)

    def count_documents(
        self,
        cls: Type[Document],
        filter: OptionalJson = None,
    ) -> int:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        return self.count_documents(filter=migo_filter)

    def bulk_write(self, _: list[PyMongoOperations]) -> BulkWriteResult:
        raise NotImplementedError

    def insert_one(
        self,
        cls: Type[Document],
        data: Json,
    ) -> InsertOneResult:
        migo_data = _build_migo_data(cls, data=data, out=MigoDocument)
        return self.__collection.insert_one(data=migo_data)

    def insert_many(
        self,
        cls: Type[Document],
        data: list[Json],
    ) -> InsertManyResult:
        migo_data = BatchDocument(mongo_documents=[], milvus_arrays=[])
        for value in data:
            migo_doc = _build_migo_data(cls, data=value, out=MigoDocument)
            if migo_doc.mongo_document is not None:
                migo_data.mongo_documents.append(migo_doc.mongo_document)
            if migo_doc.milvus_array:
                migo_data.milvus_arrays.append(migo_doc.milvus_array)

        return self.__collection.insert_many(migo_data)

    def replace_one(
        self,
        cls: Type[Document],
        filter: Json,
        replacement: Json,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        migo_doc = _build_migo_data(cls, data=replacement, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        return self.__collection.replace_one(
            data=migo_doc,
            filter=migo_filter,
            upsert=upsert,
        )

    def update_one(
        self,
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateOneResult:
        migo_doc = _build_migo_data(cls, data=update, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoDocument)
        return self.__collection.update_one(
            data=migo_doc,
            filter=migo_filter,
            upsert=upsert,
        )

    def update_many(
        self,
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateManyResult:
        migo_doc = _build_migo_data(cls, data=update, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        return self.__collection.update_many(
            data=migo_doc,
            filter=migo_filter,
            upsert=upsert,
        )

    def delete_one(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteOneResult:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        return self.__collection.delete_one(filter=migo_filter)

    def delete_many(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteManyResult:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        return self.__collection.delete_many(filter=migo_filter)
