from abc import ABC, abstractmethod

from .collection import Collection


class Database(ABC):
    @abstractmethod
    def _get_driver_database(self) -> "Database":
        pass

    @abstractmethod
    def get_collections(self) -> list[Collection]:
        pass

    @abstractmethod
    def get_collection(self, name: str) -> Collection:
        pass

    @abstractmethod
    def create_collection(self, name: str) -> bool:
        pass

    @abstractmethod
    def delete_collection(self, name: str) -> bool:
        pass

    @abstractmethod
    def __getitem__(self, name: str) -> Collection:
        pass
