from typing import Any, Dict, TypeVar

from redb.core.document import (
    Document,
    DocumentData,
    IncludeColumns,
    OptionalDocumentData,
    SortColumns,
    _format_document_data,
    _format_fields,
    _format_sort,
    _get_return_cls,
    _validate_fields,
)
from redb.interface.collection import (
    BulkWriteResult,
    Collection,
    DeleteManyResult,
    DeleteOneResult,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    ReplaceOneResult,
    UpdateManyResult,
    UpdateOneResult,
)
from redb.interface.configs import (
    CONFIG_TYPE,
    JSONConfig,
    MigoConfig,
    MongoConfig,
    check_config,
)

T = TypeVar("T", bound=Document)


class CollectionWrapper:
    def __init__(self, collection: Collection, collection_class: T) -> None:
        self.__collection = collection
        self.__collection_class = collection_class

    def _get_driver_collection(self) -> Any:
        return self.__collection

    def find_one(
        self,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> "Document":
        return_cls = _get_return_cls(self.__collection_class, fields)
        filter = _format_document_data(filter)
        fields = _format_fields(fields)
        return self.__collection.find_one(
            cls=self.__collection_class,
            return_cls=return_cls,
            filter=filter,
            skip=skip,
        )

    def find_many(
        self,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list["Document"]:
        return_cls = _get_return_cls(self.__collection_class, fields)
        filter = _format_document_data(filter)
        fields = _format_fields(fields)
        sort = _format_sort(sort)
        return self.__collection.find(
            cls=self.__collection_class,
            return_cls=return_cls,
            filter=filter,
            fields=fields,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    def distinct(
        self,
        key: str,
        filter: OptionalDocumentData = None,
    ) -> list["Document"]:
        filter = _format_document_data(filter)
        return self.__collection.distinct(
            cls=self.__collection_class,
            key=key,
            filter=filter,
        )

    def count_documents(
        self,
        filter: OptionalDocumentData = None,
    ) -> int:
        filter = _format_document_data(filter)
        return self.__collection.count_documents(
            cls=self.__collection_class,
            filter=filter,
        )

    def bulk_write(
        self,
        operations: list[PyMongoOperations],
    ) -> BulkWriteResult:
        return self.__collection.bulk_write(
            cls=self.__collection_class,
            operations=operations,
        )

    def insert_one(self, data: DocumentData) -> InsertOneResult:
        data = _format_document_data(data)
        return self.__collection.insert_one(
            cls=self.__collection_class,
            data=data,
        )

    def insert_vectors(self, data: Dict[str, list[Any]]) -> InsertManyResult:
        keys = list(data.keys())
        values_size = len(data[keys[0]])
        instances = [None] * values_size
        instances = [{key: data[key][i] for key in keys} for i in range(values_size)]
        return self.__collection.insert_many(
            cls=self.__collection_class,
            data=instances,
        )

    def insert_many(
        self,
        data: list[DocumentData],
    ) -> InsertManyResult:
        [_validate_fields(self.__collection_class, val) for val in data]

        data = [_format_document_data(val) for val in data]
        return self.__collection.insert_many(
            cls=self.__collection_class,
            data=data,
        )

    def replace_one(
        self,
        filter: DocumentData,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        if not allow_new_fields:
            _validate_fields(self.__collection_class, replacement)

        filter = _format_document_data(filter)
        replacement = _format_document_data(replacement)
        return self.__collection.replace_one(
            cls=self.__collection_class,
            filter=filter,
            replacement=replacement,
            upsert=upsert,
        )

    def update_one(
        self,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        if not allow_new_fields:
            _validate_fields(self.__collection_class, update)

        filter = _format_document_data(filter)
        update = _format_document_data(update)
        return self.__collection.update_one(
            cls=self.__collection_class,
            filter=filter,
            update=update,
            upsert=upsert,
        )

    def update_many(
        self,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> UpdateManyResult:
        if not allow_new_fields:
            _validate_fields(self.__collection_class, update)

        filter = _format_document_data(filter)
        update = _format_document_data(update)
        return self.__collection.update_many(
            cls=self.__collection_class,
            filter=filter,
            update=update,
            upsert=upsert,
        )

    def delete_one(self, filter: DocumentData) -> DeleteOneResult:
        filter = _format_document_data(filter)
        return self.__collection.delete_one(
            cls=self.__collection_class,
            filter=filter,
        )

    def delete_many(self, filter: DocumentData) -> DeleteManyResult:
        filter = _format_document_data(filter)
        return self.__collection.delete_many(
            cls=self.__collection_class,
            filter=filter,
        )


class transaction:
    def __init__(
        self,
        backend: str | None = None,
        config: CONFIG_TYPE | None = None,
        database_name: str | None = None,
        collection_name: str | None = None,
        collection_class: Document | None = None,
    ):
        new_client, client = get_client(backend, config)
        if new_client:
            self.__client = client
        else:
            self.__client = None

        if database_name is None:
            database = client.get_default_database()
        else:
            database = client.get_database(database_name)

        if collection_name is None:
            if collection_class is None:
                msg = "Neither collection_name nor collection_class provided."
                raise ValueError(msg)

            collection_name = collection_class.collection_name()
            driver_collection = database.get_collection(collection_name)
            self.__collection = CollectionWrapper(driver_collection, collection_class)
        else:
            self.__collection = database.get_collection(collection_name)

    def __enter__(self) -> Collection | CollectionWrapper:
        return self.__collection

    def __exit__(self, type, *_):
        if self.__client is not None:
            self.__client.close()

        return type is None


def get_client(backend: str, config: CONFIG_TYPE):
    if backend is None and isinstance(config, dict):
        raise ValueError("Cannot determine client type from backend and config")
    elif backend == "json" or (backend is None and check_config(config, JSONConfig)):
        from redb.json_system import JSONClient

        return True, JSONClient(config)

    elif backend == "mongo" or (backend is None and check_config(config, MongoConfig)):
        from redb.mongo_system import MongoClient

        return True, MongoClient(config)

    elif backend == "migo" and (backend is None and check_config(config, MigoConfig)):
        from redb.migo_system import MigoClient

        return True, MigoClient(config)

    else:
        from redb.core.instance import RedB

        return False, RedB.get_client()
