from .base import Base
from .insertion import InsertionMixin
from .retrieval import RetrievalMixin


class Document(Base, InsertionMixin, RetrievalMixin):
    pass
