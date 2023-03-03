class REDBError(Exception):
    """Base class for all REDB errors."""
    pass

class DocumentNotFound(REDBError):
    pass

class CannotUpdateIdentifyingField(REDBError):
    pass
