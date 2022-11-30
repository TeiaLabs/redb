from typing import Any

from ..interfaces import (
    BulkWriteResult,
    Client,
    Collection,
    Database,
    DeleteManyResult,
    DeleteOneResult,
    IncludeField,
    InsertManyResult,
    InsertOneResult,
    PyMongoOperations,
    SortField,
    UpdateManyResult,
    UpdateOneResult,
)


class MongoCollection(Collection):
    pass


class MongoDatabase(Database):
    pass


class MongoClient(Client):
    pass
