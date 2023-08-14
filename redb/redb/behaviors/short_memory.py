from typing import ClassVar, Sequence, Type, TypeVar

from redb.interface.fields import Index, PyMongoOperations
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

from ..core.document import (
    Document,
    DocumentData,
    IncludeColumns,
    OptionalDocumentData,
    SortColumns,
    _format_document_data,
)

T = TypeVar("T", bound="CachedDocument")


class CachedDocument(Document):
    memory: ClassVar[dict[str, "CachedDocument"]] = {}
    unique_fields: ClassVar[set[str]] = {"_id", "id"}

    @classmethod
    def create_indexes(cls: Type[T]) -> None:
        indexes = cls.get_indexes()
        for idx in indexes:
            if not isinstance(idx, Index):
                continue

            cls.unique_fields.add(idx.field.model_field.name)
            cls.unique_fields.add(idx.field.model_field.alias)

        return super().create_indexes()

    def find(
        self: T,
        fields: IncludeColumns = None,
        skip: int = 0,
        force: bool = False,
    ) -> T:
        if force or fields is not None:
            return super().find(fields=fields, skip=skip)

        for attr in self.__fields_set__:
            if attr not in self.unique_fields:
                continue

            field_value = getattr(self, attr)
            key = f"{attr}-{field_value}"
            memoized = self.memory.get(key, None)
            if memoized is not None:
                return memoized

        return super().find(fields=fields, skip=skip)

    @classmethod
    def find_one(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        skip: int = 0,
        force: bool = False,
    ) -> T:
        if force or fields is not None or filter is None:
            return super().find_one(filter=filter, fields=fields, skip=skip)

        filter = _format_document_data(filter)
        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue

            mem_key = f"{key}-{value}"
            memoized = cls.memory.get(mem_key, None)
            if memoized is not None:
                return memoized  # type: ignore

        return super().find_one(filter=filter, fields=fields, skip=skip)

    @classmethod
    def find_many(
        cls: Type[T],
        filter: OptionalDocumentData = None,
        fields: IncludeColumns = None,
        sort: SortColumns = None,
        skip: int = 0,
        limit: int = 0,
        force: bool = False,
    ) -> list[T]:
        if force or fields is not None or filter is None:
            return super().find_many(
                filter=filter,
                fields=fields,
                sort=sort,
                skip=skip,
                limit=limit,
            )

        filter = _format_document_data(filter)
        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue

            mem_key = f"{key}-{value}"
            memoized = cls.memory.get(mem_key, None)
            if memoized is not None:
                return memoized  # type: ignore

        return super().find_many(
            filter=filter,
            fields=fields,
            sort=sort,
            skip=skip,
            limit=limit,
        )

    @classmethod
    def bulk_write(
        cls: Type[T], operations: list[PyMongoOperations]
    ) -> BulkWriteResult:
        cls.memory.clear()
        return super().bulk_write(operations)

    def insert(self: T) -> InsertOneResult:
        self.memory[f"id-{self.id}"] = self
        self.memory[f"_id-{self.id}"] = self
        for field in self.__fields_set__:
            if field not in self.unique_fields:
                continue

            model_field = self.__fields__[field]
            field_value = getattr(self, field)
            mem_key = f"{model_field.name}-{field_value}"
            alias_mem_key = f"{model_field.alias}-{field_value}"

            self.memory[mem_key] = self
            self.memory[alias_mem_key] = self

        return super().insert()

    @classmethod
    def insert_one(cls: Type[T], data: DocumentData) -> InsertOneResult:
        data = _format_document_data(data)
        try:
            content = cls(**data)
        except:
            return super().insert_one(data)

        cls.memory[f"id-{content.id}"] = content
        cls.memory[f"_id-{content.id}"] = content
        for field in content.__fields_set__:
            if field not in cls.unique_fields:
                continue

            model_field = cls.__fields__[field]
            field_value = getattr(content, field)
            mem_key = f"{model_field.name}-{field_value}"
            alias_mem_key = f"{model_field.alias}-{field_value}"

            cls.memory[mem_key] = content
            cls.memory[alias_mem_key] = content

        return super().insert_one(data)

    @classmethod
    def insert_many(cls: Type[T], data: Sequence[DocumentData]) -> InsertManyResult:
        for doc in data:
            doc = _format_document_data(doc)
            try:
                content = cls(**doc)
            except:
                continue

            cls.memory[f"id-{content.id}"] = content
            cls.memory[f"_id-{content.id}"] = content
            for field in content.__fields_set__:
                if field not in cls.unique_fields:
                    continue

                model_field = cls.__fields__[field]
                field_value = getattr(content, field)
                mem_key = f"{model_field.name}-{field_value}"
                alias_mem_key = f"{model_field.alias}-{field_value}"

                cls.memory[mem_key] = content
                cls.memory[alias_mem_key] = content

        return super().insert_many(data)

    def replace(
        self: T,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        replacement = _format_document_data(replacement)
        for attr in self.__fields_set__:
            if attr not in self.unique_fields:
                continue

            model_field = self.__fields__[attr]
            field_value = getattr(self, attr)
            mem_key = f"{model_field.name}-{field_value}"
            alias_mem_key = f"{model_field.alias}-{field_value}"

            self.memory[mem_key] = self
            self.memory[alias_mem_key] = self

        return super().replace(replacement, upsert, allow_new_fields)

    @classmethod
    def replace_one(
        cls: Type[T],
        filter: DocumentData,
        replacement: DocumentData,
        upsert: bool = False,
        allow_new_fields: bool = False,
    ) -> ReplaceOneResult:
        filter = _format_document_data(filter)
        replacement = _format_document_data(replacement)

        try:
            replaced_obj = cls(**replacement)
        except:
            cls.memory.clear()
            return super().replace_one(filter, replacement, upsert, allow_new_fields)

        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue
            try:
                model_field = replaced_obj.__fields__[key]
            except:
                continue

            mem_key = f"{model_field.name}-{value}"
            alias_mem_key = f"{model_field.alias}-{value}"

            cls.memory[mem_key] = replaced_obj
            cls.memory[alias_mem_key] = replaced_obj

        return super().replace_one(filter, replacement, upsert, allow_new_fields)

    def update(
        self,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "",
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        update = _format_document_data(update)
        for attr in self.__fields_set__:
            if attr not in self.unique_fields:
                continue

            model_field = self.__fields__[attr]
            field_value = getattr(self, attr)
            mem_key = f"{model_field.name}-{field_value}"
            alias_mem_key = f"{model_field.alias}-{field_value}"

            self.memory[mem_key] = self
            self.memory[alias_mem_key] = self

        return super().update(
            update=update,
            upsert=upsert,
            operator=operator,
            allow_new_fields=allow_new_fields,
        )

    @classmethod
    def update_one(
        cls,
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "",
        allow_new_fields: bool = False,
    ) -> UpdateOneResult:
        filter = _format_document_data(filter)
        update = _format_document_data(update)

        try:
            updated_obj = cls(**update)
        except:
            cls.memory.clear()
            return super().update_one(
                filter=filter,
                update=update,
                upsert=upsert,
                allow_new_fields=allow_new_fields,
            )

        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue
            try:
                model_field = updated_obj.__fields__[key]
            except:
                continue

            mem_key = f"{model_field.name}-{value}"
            alias_mem_key = f"{model_field.alias}-{value}"

            cls.memory[mem_key] = updated_obj
            cls.memory[alias_mem_key] = updated_obj

        return super().update_one(
            filter=filter,
            update=update,
            upsert=upsert,
            operator=operator,
            allow_new_fields=allow_new_fields,
        )

    @classmethod
    def update_many(
        cls: Type[T],
        filter: DocumentData,
        update: DocumentData,
        upsert: bool = False,
        operator: str | None = "",
        allow_new_fields: bool = False,
    ) -> UpdateManyResult:
        cls.memory.clear()
        return super().update_many(filter, update, upsert, operator, allow_new_fields)

    def delete(self: T) -> DeleteOneResult:
        for attr in self.__fields_set__:
            if attr not in self.unique_fields:
                continue

            model_field = self.__fields__[attr]
            value = getattr(self, attr)

            mem_key = f"{model_field.name}-{value}"
            alias_mem_key = f"{model_field.alias}-{value}"

            self.memory.pop(mem_key, None)
            self.memory.pop(alias_mem_key, None)

        return super().delete()

    @classmethod
    def delete_one(cls: Type[T], filter: DocumentData) -> DeleteOneResult:
        filter = _format_document_data(filter)
        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue

            model_field = cls.__fields__[key]

            mem_key = f"{model_field.name}-{value}"
            alias_mem_key = f"{model_field.alias}-{value}"

            cls.memory.pop(mem_key, None)
            cls.memory.pop(alias_mem_key, None)

        return super().delete_one(filter=filter)

    @classmethod
    def delete_many(cls: Type[T], filter: DocumentData) -> DeleteManyResult:
        filter = _format_document_data(filter)
        for key, value in filter.items():
            if key not in cls.unique_fields:
                continue

            model_field = cls.__fields__[key]

            mem_key = f"{model_field.name}-{value}"
            alias_mem_key = f"{model_field.alias}-{value}"

            cls.memory.pop(mem_key, None)
            cls.memory.pop(alias_mem_key, None)

        return super().delete_many(filter=filter)
