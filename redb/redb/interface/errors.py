class REDBError(Exception):
    """Base class for all REDB errors."""

    def __init__(self, *args: object, collection_name: str = "") -> None:
        super().__init__(*args)
        self.collection_name = collection_name

    pass


class DocumentNotFound(REDBError):
    pass


class CannotUpdateIdentifyingField(REDBError):
    pass


class UniqueConstraintViolation(REDBError):
    def __init__(
        self, *args: object, dup_keys: dict, collection_name: str = ""
    ) -> None:
        if collection_name:
            msg = f"Duplicate key at collection {collection_name} on: {dup_keys}"
        else:
            msg = f"Duplicate key error on: {dup_keys}"
        super().__init__(msg, *args, collection_name=collection_name)
        self.dup_keys = [dup_keys]


class UnsupportedOperation(REDBError):
    pass
