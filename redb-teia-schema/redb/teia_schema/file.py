from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from redb.core import Document
from redb.interface.fields import ClassField, CompoundIndex, Index

from .instance import Instance


class Mirror(BaseModel):
    source: str
    url: str


class File(Document):
    scraped_at: datetime
    last_modified_at: datetime
    hash: str
    organization_id: str
    size_bytes: int
    url_mirrors: Optional[list[Mirror]] = None
    url_original: str

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        fields = [
            cls.hash,
            cls.organization_id,
            cls.size_bytes,
            cls.url_original,
        ]
        return fields

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return [Index(field=cls.id)]  # type: ignore

    @classmethod
    def get_organization_ids(cls) -> set:
        return set(cls.distinct(key="organization_id"))

    @classmethod
    def get_file_instances(cls, file_id: str) -> list[Instance]:
        return Instance.find_many({"file_id": file_id})
