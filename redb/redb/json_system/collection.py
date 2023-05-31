import json
import sys
from pathlib import Path
from typing import Any, Type

from redb.core import BaseDocument, Document
from redb.interface.errors import DocumentNotFound
from redb.interface.collection import Collection, Json, OptionalJson, ReturnType
from redb.interface.fields import CompoundIndex, PyMongoOperations
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


class JSONCollection(Collection):
    __client_name__ = "json"

    def __init__(self, collection: Path, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__collection = collection

    def _get_driver_collection(self):
        return self.__collection

    def create_index(
        self,
        index: CompoundIndex,
    ) -> bool:
        raise NotImplementedError

    def find(
        self,
        cls: Type[Document],
        return_cls: Type[ReturnType],
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        sort: list[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        transform = lambda file_path: json.load(open(file_path))
        if filter is not None and "_id" in filter:
            file_path = self.__collection / f"{filter['_id']}.json"
            if file_path.is_file() and not file_path.is_symlink():
                return [return_cls(**transform(file_path))]

            if len(filter.keys()) == 1:
                # If the only filter was the ID, return empty list
                return []

        json_files = self.__collection.glob("*.json")
        out = []
        for i, json_file in enumerate(json_files):
            if i < skip:
                continue
            if limit and len(out) >= limit:
                break
            if not json_file.is_file():
                continue

            transformed_json: dict = transform(json_file)
            if filter is not None:
                ignore_file = False
                for key in filter:
                    if key not in transformed_json:
                        ignore_file = True
                        break
                    if transformed_json[key] != filter[key]:
                        ignore_file = True
                        break
                if ignore_file:
                    continue

            if fields is not None:
                transformed_json = {
                    key: value
                    for key, value in transformed_json.items()
                    if key in fields
                }

            out.append(return_cls(**transformed_json))

        return out

    def find_one(
        self,
        cls: Type[Document],
        return_cls: Type[ReturnType],
        fields: dict[str, bool] | None = None,
        filter: OptionalJson = None,
        skip: int = 0,
    ) -> ReturnType:
        captures = self.find(
            cls,
            return_cls,
            filter=filter,
            fields=fields,
            skip=skip,
            limit=1,
        )
        return captures[0]

    def distinct(
        self,
        cls: Type[ReturnType],
        key: str,
        filter: OptionalJson = None,
    ) -> list[Any]:
        docs = self.find(
            cls,
            return_cls=dict,
            filter=filter,
        )
        computed_values = set()
        for doc in docs:
            if doc[key] in computed_values:
                continue

            computed_values.add(doc[key])

        return list(computed_values)

    def count_documents(
        self,
        cls: Type[Document],
        filter: OptionalJson = None,
    ) -> int:
        return len(self.find(cls, return_cls=dict, filter=filter))

    def bulk_write(self, _: list[PyMongoOperations]) -> BulkWriteResult:
        raise NotImplementedError

    def insert_one(
        self,
        cls: Type[Document],
        data: Json,
    ) -> InsertOneResult:
        self.__collection.mkdir(parents=True, exist_ok=True)

        id = data["_id"]
        json_path = self.__collection / Path(f"{id}.json")
        if json_path.is_file():
            raise ValueError(f"Document with {id} already exists")

        with open(json_path, "w") as f:
            json.dump(data, f, indent=4)

        return InsertOneResult(inserted_id=id)

    def insert_many(
        self,
        cls: Type[Document],
        data: list[Json],
    ) -> InsertManyResult:
        ids = []
        for item in data:
            ids.append(self.insert_one(cls, data=item).inserted_id)

        return InsertManyResult(inserted_ids=ids)

    def replace_one(
        self,
        cls: Type[Document],
        filter: Json,
        replacement: Json,
        upsert: bool = False,
    ) -> ReplaceOneResult:
        doc = self.find_one(cls, return_cls=dict, filter=filter)
        if doc is None:
            if not upsert:
                raise ValueError(f"Document not found")

            self.insert_one(cls, data=replacement)
            return ReplaceOneResult(
                matched_count=1,
                modified_count=1,
                upserted_id=replacement["_id"],
            )

        original_path = self.__collection / Path(f"{doc['_id']}.json")
        upserted = False
        if replacement["_id"] != doc["_id"]:
            # Since the ID has changed, we need to remove the old one
            original_path.unlink()
            upserted = True

        new_path = self.__collection / Path(f"{replacement['_id']}.json")
        with open(new_path, "w") as f:
            json.dump(replacement, f, indent=4)

        return ReplaceOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=replacement["_id"] if upserted else None,
        )

    def update_one(
        self,
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateOneResult:
        doc: dict = self.find_one(cls, return_cls=dict, filter=filter)
        update = update.pop(next(iter(update.keys())))  # { $set: {...} }
        if doc is None:
            if not upsert:
                raise DocumentNotFound(collection_name=cls.collection_name())
            result = self.insert_one(cls, data=update)
            return UpdateOneResult(
                matched_count=1,
                modified_count=1,
                upserted_id=result.inserted_id,
            )
        doc_path = self.__collection / Path(f"{doc['_id']}.json")
        updated_content = doc | update
        with open(doc_path, "w") as f:
            json.dump(updated_content, f, indent=2)
        return UpdateOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=None,
        )

    def update_many(
        self,
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateManyResult:
        upserted_ids = []
        docs = self.find(cls, return_cls=dict, filter=filter)
        if not docs:
            if not upsert:
                raise ValueError(f"Document not found")

            self.insert_one(cls, data=update)
            return UpdateManyResult(
                matched_count=1,
                modified_count=1,
                upserted_id=update["_id"],
            )

        for doc in docs:
            original_path = self.__collection / Path(f"{doc['_id']}.json")
            with open(original_path, "r") as f:
                original_content: dict = json.load(f)

            original_content.update(update)
            if "_id" in update:
                new_id = update["_id"]
            elif cls is dict:
                new_id = BaseDocument().get_dict_hash(
                    data=original_content, use_data_fields=True
                )
            else:
                new_id = cls.get_hash(data=original_content)

            if doc["_id"] != new_id:
                # Since the ID has changed, we need to remove the old one
                original_path.unlink()
                upserted_ids.append(new_id)

            original_content["_id"] = new_id
            new_path = self.__collection / Path(f"{new_id}.json")
            with open(new_path, "w") as f:
                json.dump(original_content, f, indent=4)

        return UpdateManyResult(
            matched_count=len(filter),
            modified_count=len(upserted_ids),
            upserted_id=upserted_ids,
        )

    def delete_one(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteOneResult:
        doc = self.find_one(cls, return_cls=dict, filter=filter)
        if doc is None:
            raise ValueError(f"Document not found")

        file = self.__collection / f"{doc['_id']}.json"
        file.unlink()
        return DeleteOneResult(deleted_count=1)

    def delete_many(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteManyResult:
        docs = self.find(cls, return_cls=dict, filter=filter)
        for doc in docs:
            file = self.__collection / f"{doc['_id']}.json"
            file.unlink()

        return DeleteManyResult(deleted_count=len(docs))
