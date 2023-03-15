from functools import lru_cache
from typing import Any, Type, TypeVar

from migo.collection import BatchDocument
from migo.collection import Collection as MigoDriverCollection
from migo.collection import Document as MigoDocument
from migo.collection import Field as MigoField
from migo.collection import Filter as MigoFilter
from migo.utils import (
    AnnoyIndex,
    BINFlatIndex,
    BINIVFIndex,
    DISKANNIndex,
    FlatIndex,
    HNSWINdex,
)
from migo.utils import Index as MigoIndex
from migo.utils import (
    IVFFlatIndex,
    IVFPQIndex,
    IVFSQ8Index,
    MilvusBinaryIndex,
    MilvusFloatingIndex,
    MongoGeoIndex,
    MongoIndex,
)

from redb.core import Document
from redb.interface.collection import Collection, Json, OptionalJson, ReturnType
from redb.interface.fields import (
    ClassField,
    CompoundIndex,
    Direction,
    PyMongoOperations,
)
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


def _is_milvus_field(field: ClassField) -> bool:
    return (
        hasattr(field.model_field, "vector_type")
        and field.model_field.vector_type is not None
    )


def _build_milvus_index_type(params: dict):
    if params["index_type"] == "FLAT":
        index_type = FlatIndex()
    elif params["index_type"] == "IVF_FLAT":
        index_type = IVFFlatIndex(nlist=params["nlist"])
    elif params["index_type"] == "IVF_SQ8":
        index_type = IVFSQ8Index(nlist=params["nlist"])
    elif params["index_type"] == "IVF_PQ":
        index_type = IVFPQIndex(
            nlist=params["nlist"],
            m=params["m"],
            nbits=params["nbits"],
        )
    elif params["index_type"] == "HNSW":
        index_type = HNSWINdex(
            M=params["M"],
            efConstruction=params["efCOnstruction"],
        )
    elif params["index_type"] == "ANNOY":
        index_type = AnnoyIndex(n_trees=params["n_trees"])
    elif params["index_type"] == "DISKANN*":
        index_type = DISKANNIndex()
    elif params["index_type"] == "BIN_FLAT":
        index_type = BINFlatIndex
    elif params["index_type"] == "BIN_IVF_FLAT":
        index_type = BINIVFIndex(nlist=params["nlist"])
    else:
        raise ValueError(f'Unknown index type: {params["index_type"]}')

    return index_type


def _build_index_name(index: CompoundIndex, fields: list[ClassField]) -> str:
    if index.name is None:
        index.name = "_".join([field.join_attrs("_") for field in fields])
        index.name = f"unique_{index.name}" if index.unique else index.name
        index.name = f"{index.direction.name.lower()}_{index.name}"

    return index.name


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
            if _is_milvus_field(field):
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
            if _is_milvus_field(field):
                migo_fields.append(MigoField(milvus_field=name))
            else:
                migo_fields.append(MigoField(mongo_field=name))

    return migo_fields


def _build_mongo_index(index: CompoundIndex) -> MongoIndex | MongoGeoIndex | None:
    mongo_fields: list[ClassField] = []
    for field in index.fields:
        if not _is_milvus_field(field):
            mongo_fields.append(field)

    if not mongo_fields:
        return None

    name = _build_index_name(index, mongo_fields)
    mongo_index = dict(
        key=[(field.join_attrs(), index.direction) for field in mongo_fields],
        name=name,
        unique=index.unique,
        type=index.direction,
        sparse=index.extras.get("sparse", False),
        expiration_secs=index.extras.get("expiration_secs", None),
        hidden=index.extras.get("hidden", None),
    )

    if index.direction in {Direction.GEO2D, Direction.GEOSPHERE}:
        mongo_index = MongoGeoIndex(
            bucket_size=index.extras.get("bucket_size", None),
            min=index.extras.get("min", None),
            max=index.extras.get("max", None),
            **mongo_index,
        )
    else:
        mongo_index = MongoIndex(**mongo_index)

    return mongo_index


def _build_milvus_indexes(
    index: CompoundIndex,
) -> list[MilvusBinaryIndex | MilvusFloatingIndex]:
    milvus_fields: list[ClassField] = []
    for field in index.fields:
        if _is_milvus_field(field):
            milvus_fields.append(field)

    if not milvus_fields:
        return []

    milvus_indexes = []
    for milvus_field in milvus_fields:
        name = _build_index_name(index, milvus_fields)
        index_type = _build_milvus_index_type(index.extras)
        milvus_index = dict(
            key=milvus_field.join_attrs(),
            name=name,
            metric_type=index.extras["metric_type"],
            index_type=index_type,
        )
        if type(index_type) in {BINFlatIndex, BINIVFIndex}:
            milvus_indexes.append(MilvusBinaryIndex(**milvus_index))
        else:
            milvus_indexes.append(MilvusFloatingIndex(**milvus_index))

    return milvus_indexes


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
        mongo_index = _build_mongo_index(index)
        milvus_indexes = _build_milvus_indexes(index)
        try:
            self.__collection.create_indexes(
                [MigoIndex(mongo_index=mongo_index, milvus_indexes=milvus_indexes)]
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
        sort: list[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        migo_fields = _build_migo_fields(cls, fields)
        results = self.__collection.find_many(
            filter=migo_filter,
            fields=migo_fields,
            sort=sort,
            limit=limit,
        )
        return [return_cls(**result) for result in results]

    def find_one(
        self,
        cls: Type[Document],
        return_cls: ReturnType,
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        skip: int = 0,
    ) -> ReturnType:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        migo_fields = _build_migo_fields(cls, fields)
        result = self.__collection.find_one(filter=migo_filter, fields=migo_fields)
        return return_cls(**result)

    def distinct(
        self,
        cls: ReturnType,
        key: str,
        filter: OptionalJson = None,
    ) -> list[Any]:
        results = self.__collection.distinct(key=key, filter=filter)
        return results

    def count_documents(
        self,
        cls: Type[Document],
        filter: OptionalJson = None,
    ) -> int:
        return self.__collection.count(filter=filter)

    def bulk_write(self, _: list[PyMongoOperations]) -> BulkWriteResult:
        raise NotImplementedError

    def insert_one(
        self,
        cls: Type[Document],
        data: Json,
    ) -> InsertOneResult:
        migo_data = _build_migo_data(cls, data=data, out=MigoDocument)
        result = self.__collection.insert_one(data=migo_data)
        return InsertOneResult(inserted_id=result.inserted_id)

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

        result = self.__collection.insert_many(migo_data)
        return InsertManyResult(inserted_ids=result.inserted_ids)

    def replace_one(
        self,
        cls: Type[Document],
        filter: Json,
        replacement: Json,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        migo_doc = _build_migo_data(cls, data=replacement, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        result = self.__collection.replace_one(
            data=migo_doc,
            filter=migo_filter,
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
        migo_doc = _build_migo_data(cls, data=update, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoDocument)
        result = self.__collection.update_one(
            data=migo_doc,
            filter=migo_filter,
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
        migo_doc = _build_migo_data(cls, data=update, out=MigoDocument)
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        result = self.__collection.update_many(
            data=migo_doc,
            filter=migo_filter,
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
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        result = self.__collection.delete_one(filter=migo_filter)
        return DeleteOneResult(deleted_count=result.deleted_count)

    def delete_many(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteManyResult:
        migo_filter = _build_migo_data(cls, data=filter, out=MigoFilter)
        result = self.__collection.delete_many(filter=migo_filter)
        return DeleteManyResult(deleted_count=result.deleted_count)
