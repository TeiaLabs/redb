class REDBError(Exception):
    """Base class for all REDB errors."""

    pass


class DocumentNotFound(REDBError):
    pass


class CannotUpdateIdentifyingField(REDBError):
    pass


class UniqueConstraintViolation(REDBError):
    """Raised when an insert or update fails due to a duplicate key error."""

    def __init__(self, *args: object, dup_keys: dict) -> None:
        msg = f"Duplicate key error on: {dup_keys}"
        super().__init__(msg, *args)
