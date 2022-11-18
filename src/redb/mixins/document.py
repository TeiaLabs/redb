from .insertion import InsertionMixin
from .retrieval import RetrievalMixin


class Document(InsertionMixin, RetrievalMixin):
    pass
