from typing import Type, TypeVar

from redb.core import transaction

T = TypeVar("T")


class SwitcharooMixin:

    @classmethod
    def switch_db(cls: Type[T], db_name: str) -> Type[T]:
        with transaction.transaction(cls, db_name=db_name) as new_cls:
            return new_cls
