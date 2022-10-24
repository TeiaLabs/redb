from typing import Literal, Type, TypeVar

from .client import Client, JSONClient, MongoClient
from .document import Document
from .json_system.document import JSONDocument

C = TypeVar("C", bound=Client)
D = TypeVar("D", bound=Type[Document])


class REDB:
    """Client singleton."""

    client: C | None = None
    _doc_class: D | None = None

    def __init__(self, backend: Literal["json", "mongo"], kwargs):
        client_class, REDB._doc_class = self.choose_engine(backend)
        REDB.client = client_class(**kwargs)

    @classmethod
    def get_client(cls):
        if cls.client is None:
            raise RuntimeError
        return cls.client

    @classmethod
    def get_doc_class(cls):
        if cls._doc_class is None:
            raise RuntimeError
        return cls._doc_class

    @classmethod
    def choose_engine(cls, backend: str) -> tuple[Type[Client], Type[Document]]:
        client_class: Type[Client]
        document_class: Type[Document]
        if backend == "json":
            client_class = JSONClient
            document_class = JSONDocument
        elif backend == "mongo":
            client_class = MongoClient
            document_class = JSONDocument  # TODO
        else:
            raise ValueError(f"Backend {backend!r} not found.")
        return client_class, document_class
