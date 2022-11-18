from typing import Any

from ..redb.interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    SortField,
    UpdateResult,
)


class MongoCollection(Collection):
    pass


class MongoDatabase(Database):
    pass


class MongoClient(Client):
    pass
