from abc import ABC, abstractmethod
from typing import Sequence

from .database import Database


class Client(ABC):
    @abstractmethod
    def _get_driver_client(self) -> "Client":
        pass

    @abstractmethod
    def get_default_database(self) -> Database:
        pass

    @abstractmethod
    def get_databases(self) -> Sequence[Database]:
        pass

    @abstractmethod
    def get_database(self, name: str) -> Database:
        pass

    @abstractmethod
    def drop_database(self, name: str) -> bool:
        pass

    @abstractmethod
    def close(self) -> bool:
        pass
