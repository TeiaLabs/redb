import contextlib
from datetime import datetime
from typing import Any, ContextManager, Dict, Sequence, Type, TypeVar, overload

import pytz
from pymongo.errors import DuplicateKeyError

from redb.behaviors import IRememberDoc
from redb.core.document import (
    Document,
    DocumentData,
    IncludeColumns,
    OptionalDocumentData,
    SortColumns,
    _format_document_data,
    _format_fields,
    _format_index,
    _format_sort,
    _get_return_cls,
    _optimize_filter,
    _raise_if_updating_hashable,
    _validate_fields,
)
from redb.core.instance import RedB
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
from redb.interface.errors import UniqueConstraintViolation
from redb.mongo_system import MongoClient

T = TypeVar("T", bound=Document)


class CollectionWrapper:
    def __init__(
        self,
        collection: Collection,
        history_collection: Collection,
        collection_class: T,
    ) -> None:
        self.__collection = collection
        self.__history_collection = history_collection
        self.__collection_class = collection_class

    def _get_driver_collection(self) -> Any:
        return self.__collection._get_driver_collection()

    def switch_db(self, db_name: str) -> "CollectionWrapper":
        collection_name = self.__collection_class.collection_name()

        client = self.__collection.database.client  # type: ignore
        db = client.get_database(db_name)
        collection = db.get_collection(collection_name)

        self.__collection = collection

        return self

    def switch_client(
        self,
        config: MongoConfig | dict | None = None,
        alias: str | None = None,
    ) -> "CollectionWrapper":
        collection_name = self.__collection_class.collection_name()

        client = RedB.add_client(config=config, alias=alias)

        old_db_name = self.__collection.database.name  # type: ignore
        try:
            db = client.get_database(old_db_name)
        except:
            db = client.get_default_database()

        collection = db.get_collection(collection_name)

        self.__collection = collection
        return self

    def switch(
        self,
        db: str | None = None,
        config: MongoConfig | dict | None = None,
        alias: str | None = None,
    ) -> "CollectionWrapper":
        collection_name = self.__collection_class.collection_name()
        database_name = self.__collection.database.name  # type: ignore

        if config is not None or alias is not None:
            client = RedB.add_client(config=config, alias=alias)
        else:
            client = self.__collection.database.client  # type: ignore

        if db is not None:
            _db = client.get_database(db)
        else:
            try:
                _db = client.get_database(database_name)
            except:
                _db = client.get_default_database()

        collection = _db.get_collection(collection_name)

        self.__collection = collection
        return self

    def create_indexes(self) -> None:
        indexes = self.__collection_class.get_indexes()
        for index in indexes:
            index = _format_index(index)
            self.__collection.create_index(index)

    def find_one(
        self,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
    ) -> "Document":
        chosen_fields = _format_fields(fields)
        return_cls = _get_return_cls(self.__collection_class, chosen_fields)
        filters = _format_document_data(filter)
        return self.__collection.find_one(
            cls=self.__collection_class,
            return_cls=return_cls,
            filter=filters,
            skip=skip,
            fields=chosen_fields,
        )

    def find_many(
        self,
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list["Document"]:
        chosen_fields = _format_fields(fields)
        return_cls = _get_return_cls(self.__collection_class, chosen_fields)
        filter = _format_document_data(filter)
        sort = _format_sort(sort)
        return self.__collection.find(
            cls=self.__collection_class,
            return_cls=return_cls,
            filter=filter,
            fields=chosen_fields,
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

        try:
            return self.__collection.insert_one(
                cls=self.__collection_class,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(dup_keys=e.details["keyValue"])

    def _historical_insert_one(self, data: DocumentData) -> InsertOneResult:
        if self.__history_collection is None:
            raise ValueError("Base class does not inherit from IRememberDoc")

        data = _format_document_data(data)
        try:
            return self.__history_collection.insert_one(
                cls=self.__collection_class,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(
                dup_keys=e.details["keyValue"],  # type: ignore
                collection_name=self.__collection_class.history_collection_name(),
            )

    def insert_vectors(self, data: Dict[str, list[Any]]) -> InsertManyResult:
        keys = list(data.keys())
        values_size = len(data[keys[0]])
        instances = [None] * values_size
        instances = [{key: data[key][i] for key in keys} for i in range(values_size)]

        try:
            return self.__collection.insert_many(
                cls=self.__collection_class,
                data=instances,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(dup_keys=e.details["keyValue"])

    def insert_many(
        self,
        data: Sequence[DocumentData],
    ) -> InsertManyResult:
        [_validate_fields(self.__collection_class, val) for val in data]

        data = [_format_document_data(val) for val in data]

        try:
            return self.__collection.insert_many(
                cls=self.__collection_class,
                data=data,
            )
        except DuplicateKeyError as e:
            raise UniqueConstraintViolation(dup_keys=e.details["keyValue"])

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
        operator: str | None = "$set",
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        if not allow_new_fields:
            _validate_fields(self.__collection_class, update)

        filter = _format_document_data(filter)
        update = _format_document_data(update)

        if not upsert:
            filter = _optimize_filter(self.__collection_class, filter)

        _raise_if_updating_hashable(self.__collection_class, update)
        if operator is not None:
            update = {operator: update}

        result = self.__collection.update_one(
            cls=self.__collection_class,
            filter=filter,
            update=update,
            upsert=upsert,
        )
        self.__collection.update_one(
            cls=self.__collection_class,
            filter=filter,
            update={"$set": {"updated_at": datetime.now(pytz.UTC).isoformat()}},
        )
        return result

    def historical_update_one(
        self,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
        user_info: Any = None,
    ) -> UpdateOneResult:
        if self.__history_collection is None:
            raise ValueError("Base class does not inherit from IRememberDoc")

        original_doc = self.find_one(filter=filter)
        new_history = self.__collection_class._build_history_from_ref(
            user_info, original_doc
        )
        update_result = self.update_one(
            filter={"_id": original_doc.id},
            update=update,
            upsert=upsert,
            operator=operator,
            allow_new_fields=allow_new_fields,
        )
        self._historical_insert_one(new_history)
        return update_result

    def update_many(
        self,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "$set",
        allow_new_fields: bool = False,
    ) -> UpdateManyResult:
        if not allow_new_fields:
            _validate_fields(self.__collection_class, update)

        filter = _format_document_data(filter)
        update = _format_document_data(update)

        if not upsert:
            filter = _optimize_filter(self.__collection_class, filter)

        _raise_if_updating_hashable(self.__collection_class, update)
        if operator is not None:
            update = {operator: update}

        result = self.__collection.update_many(
            cls=self.__collection_class,
            filter=filter,
            update=update,
            upsert=upsert,
        )
        self.__collection.update_many(
            cls=self.__collection_class,
            filter=filter,
            update={"$set": {"updated_at": datetime.now(pytz.UTC).isoformat()}},
        )
        return result

    def delete_one(self, filter: DocumentData) -> DeleteOneResult:
        filter = _format_document_data(filter)
        return self.__collection.delete_one(
            cls=self.__collection_class,
            filter=filter,
        )

    def historical_delete_one(
        self,
        filter: DocumentData,
        user_info: Any = None,
    ) -> DeleteOneResult:
        if self.__history_collection is None:
            raise ValueError("Base class does not inherit from IRememberDoc")

        original_doc = self.find_one(filter=filter)
        new_history = self.__collection_class._build_history_from_ref(
            user_info, original_doc
        )
        delete_result = self.delete_one(filter={"_id": original_doc.id})
        self._historical_insert_one(new_history)
        return delete_result

    def delete_many(self, filter: DocumentData) -> DeleteManyResult:
        filter = _format_document_data(filter)
        return self.__collection.delete_many(
            cls=self.__collection_class,
            filter=filter,
        )


@overload
def transaction(
    collection: Type[Document],
    backend: str | None = None,
    config: CONFIG_TYPE | None = None,
    db_name: str | None = None,
) -> ContextManager[CollectionWrapper]:
    pass


@overload
def transaction(
    collection: str,
    backend: str | None = None,
    config: CONFIG_TYPE | None = None,
    db_name: str | None = None,
) -> ContextManager[Collection]:
    pass


@contextlib.contextmanager
def transaction(
    collection: Document | str,
    backend: str | None = None,
    config: CONFIG_TYPE | None = None,
    db_name: str | None = None,
) -> Collection | CollectionWrapper:
    new_client, client = get_client(backend, config)

    if db_name is None:
        database = client.get_default_database()
    else:
        database = client.get_database(db_name)

    if isinstance(collection, str):
        collection = database.get_collection(collection)
    else:
        collection_name = collection.collection_name()
        driver_history_collection = None
        if issubclass(collection, IRememberDoc):
            history_collection_name = collection.history_collection_name()
            driver_history_collection = database.get_collection(history_collection_name)

        driver_collection = database.get_collection(collection_name)
        collection = CollectionWrapper(
            driver_collection, driver_history_collection, collection
        )

    yield collection

    if new_client:
        client.close()


def get_client(backend: str | None, config: CONFIG_TYPE):
    if backend == "json" or (backend is None and check_config(config, JSONConfig)):
        from redb.json_system import JSONClient

        return True, JSONClient(config)

    elif backend == "mongo" or (backend is None and check_config(config, MongoConfig)):
        from redb.core.instance import RedB

        client = RedB.add_client(config)
        return True, client

    elif backend == "migo" and (backend is None and check_config(config, MigoConfig)):
        from redb.migo_system import MigoClient

        return True, MigoClient(config)

    else:
        from redb.core.instance import RedB

        return False, RedB.get_client()
