import json
from typing import TypeVar, Type

from ..document import Document
from .. import init_db

T = TypeVar("T")


class JSONDocument(Document):

    @classmethod
    def find_by_id(cls: Type[T], _id: str) -> T:
        ...

    def insert(self):
        c = init_db.REDB.get_client()
        print(c.attrs["file_path"])
        print(self)
