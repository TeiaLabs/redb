class REDBError(Exception):
    """Base class for all REDB errors."""

    pass


class DocumentNotFound(REDBError):
    pass


class CannotUpdateIdentifyingField(REDBError):
    pass


class UniqueConstraintViolation(REDBError):
    """Raised when an insert or update fails due to a duplicate key error."""

    def __init__(
        self, *args: object, dup_keys: dict, collection_name: str = ""
    ) -> None:
        if collection_name:
            msg = f"Duplicate key at collection {collection_name} on: {dup_keys}"
        else:
            msg = f"Duplicate key error on: {dup_keys}"
        super().__init__(msg, *args)
