import json
from pathlib import Path
from typing import Type

from redb.core import BaseDocument, Document
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
        return_cls: ReturnType,
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        sort: dict[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        transform = lambda file_path: json.load(open(file_path))
        if filter is not None and "id" in filter:
            file_path = self.__collection / f"{filter['id']}.json"
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
        return_cls: ReturnType,
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
        if captures:
            return captures[0]
        return None

    def distinct(
        self,
        cls: ReturnType,
        key: str,
        filter: OptionalJson = None,
        fields: dict[str, bool] | None = None,
        sort: dict[tuple[str, str | int]] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> list[ReturnType]:
        if key not in fields:
            fields[key] = True

        docs = self.find(
            cls,
            return_cls=dict,
            filter=filter,
            fields=fields,
            sort=sort,
            skip=skip,
            limit=limit,
        )
        computed_values = set()
        out = []
        for doc in docs:
            if doc[key] in computed_values:
                continue

            computed_values.add(doc[key])
            out.append(cls(**doc))

        return out

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

        id = data["id"]
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
                upserted_id=replacement["id"],
            )

        original_path = self.__collection / Path(f"{doc['id']}.json")
        upserted = False
        if replacement["id"] != doc["id"]:
            # Since the ID has changed, we need to remove the old one
            original_path.unlink()
            upserted = True

        new_path = self.__collection / Path(f"{replacement['id']}.json")
        with open(new_path, "w") as f:
            json.dump(replacement, f, indent=4)

        return ReplaceOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=replacement["id"] if upserted else None,
        )

    def update_one(
        self,
        cls: Type[Document],
        filter: Json,
        update: Json,
        upsert: bool = False,
    ) -> UpdateOneResult:
        doc = self.find_one(cls, return_cls=dict, filter=filter)
        if doc is None:
            if not upsert:
                raise ValueError(f"Document not found")

            self.insert_one(cls, data=update)
            return UpdateOneResult(
                matched_count=1,
                modified_count=1,
                upserted_id=update["id"],
            )

        original_path = self.__collection / Path(f"{doc['id']}.json")
        with open(original_path, "r") as f:
            original_content: dict = json.load(f)

        original_content.update(update)
        if "id" in update:
            new_id = update["id"]
        elif cls is dict:
            new_id = BaseDocument().get_hash(
                data=original_content, use_data_fields=True
            )
        else:
            new_id = cls.get_hash(data=original_content)

        upserted = False
        if doc["id"] != new_id:
            # Since the ID has changed, we need to remove the old one
            original_path.unlink()
            upserted = True

        original_content["id"] = new_id
        new_path = self.__collection / Path(f"{new_id}.json")
        with open(new_path, "w") as f:
            json.dump(original_content, f, indent=4)

        return UpdateOneResult(
            matched_count=1,
            modified_count=1,
            upserted_id=new_id if upserted else None,
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
                upserted_id=update["id"],
            )

        for doc in docs:
            original_path = self.__collection / Path(f"{doc['id']}.json")
            with open(original_path, "r") as f:
                original_content: dict = json.load(f)

            original_content.update(update)
            if "id" in update:
                new_id = update["id"]
            elif cls is dict:
                new_id = BaseDocument().get_dict_hash(
                    data=original_content, use_data_fields=True
                )
            else:
                new_id = cls.get_hash(data=original_content)

            if doc["id"] != new_id:
                # Since the ID has changed, we need to remove the old one
                original_path.unlink()
                upserted_ids.append(new_id)

            original_content["id"] = new_id
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

        file = self.__collection / f"{doc['id']}.json"
        file.unlink()
        return DeleteOneResult()

    def delete_many(
        self,
        cls: Type[Document],
        filter: Json,
    ) -> DeleteManyResult:
        docs = self.find(cls, return_cls=dict, filter=filter)
        for doc in docs:
            file = self.__collection / f"{doc['id']}.json"
            file.unlink()

        return DeleteManyResult(deleted_count=len(docs))
