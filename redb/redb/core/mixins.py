from typing import Type, TypeVar

from redb.core import RedB
from redb.core.transaction import CollectionWrapper, transaction
from redb.interface.configs import MongoConfig
from redb.interface.errors import UnsupportedOperation
from redb.mongo_system import MongoClient

T = TypeVar("T")


class SwitcharooMixin:
    @classmethod
    def switch_db(cls: Type, db_name: str) -> CollectionWrapper:
        with transaction(cls, db_name=db_name) as new_cls:
            return new_cls

    @classmethod
    def switch_client(cls: Type, config: MongoConfig) -> CollectionWrapper:
        if RedB.get_client_name() != "mongo":
            raise UnsupportedOperation("Only Mongo flavor support client switch")

        with transaction(cls, backend="mongo", config=config) as new_cls:
            return new_cls

    @classmethod
    def switch(
        cls: Type,
        db: str | None = None,
        config: MongoConfig | dict | None = None,
        alias: str | None = None,
    ) -> CollectionWrapper:
        if RedB.get_client_name() != "mongo" and config is not None:
            raise UnsupportedOperation("Only Mongo flavor support client switch")

        with transaction(cls) as new_cls:
            return new_cls.switch(db=db, config=config, alias=alias)
