from dataclasses import dataclass
from typing import Any


@dataclass
class BulkWriteResult:
    deleted_count: int
    inserted_count: int
    matched_count: int
    modified_count: int
    upserted_count: int
    upserted_ids: int


@dataclass
class UpdateOneResult:
    matched_count: int
    modified_count: int
    upserted_id: Any


@dataclass
class UpdateManyResult(UpdateOneResult):
    pass


@dataclass
class ReplaceOneResult(UpdateOneResult):
    pass


@dataclass
class DeleteOneResult:
    deleted_count: int


@dataclass
class DeleteManyResult:
    deleted_count: int


@dataclass
class InsertManyResult:
    inserted_ids: list[Any]


@dataclass
class InsertOneResult:
    inserted_id: Any
