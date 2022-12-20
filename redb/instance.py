import inspect
from datetime import datetime
from typing import Any, Literal, Type, TypeVar

from bson import ObjectId
from pydantic.main import ModelMetaclass

from .interfaces import Client, Collection, Direction, Field, FieldIndice
from .interfaces.fields import Index
from .json_system import JSONClient, JSONCollection, JSONConfig
from .milvus_system import MilvusClient, MilvusCollection, MilvusConfig
from .mongo_system import MongoClient, MongoCollection, MongoConfig

processed_classes = {"Document"}
NON_IHERITABLE_METHODS = {"__dict__", "__abstractmethods__", "__client_name__"}

ConfigsType = TypeVar("ConfigsType", MongoConfig, JSONConfig, dict[str, Any])


def check_config(
    config,
    base_class,
) -> bool:
    return isinstance(config, base_class) or isinstance(config, dict)


def process_indices(clsname: str, attrs: dict[str, Any]) -> None:
    if clsname not in RedB._indices:
        RedB._indices[clsname] = {}

    for attr, field in attrs.items():
        if not attr.startswith("__") and hasattr(field, "index"):
            index = field.index
            index.group_name = attr if index.group_name is None else index.group_name
            if index.group_name not in RedB._indices[clsname]:
                RedB._indices[clsname][index.group_name] = []

            index.name = attr if index.name is None else index.name
            RedB._indices[clsname][index.group_name].append(index)


class RedB:
    """Client singleton."""

    _indices: dict[str, dict[str, list[FieldIndice]]] = {}
    _processed_classes: set[str] = {
        "Document",
        "BaseCollection",
        "JSONCollection",
        "MongoCollection",
    }
    _client: Client | None = None
    _sub_classes: list[Type[Collection]] | None = None

    @classmethod
    def get_client(cls, client_name: str) -> Client:
        from .json_system import JSONClient
        from .mongo_system import MongoClient

        if cls._client is None:
            raise RuntimeError("Client not setup. Call setup() first.")

        incompatible_error_message = (
            f"Current client does not match required client: {client_name}"
        )
        if isinstance(cls._client, MongoClient) and client_name != "mongo":
            raise ValueError(incompatible_error_message)
        elif isinstance(cls._client, JSONClient) and client_name != "json":
            raise ValueError(incompatible_error_message)
        return cls._client

    @classmethod
    def setup(
        cls,
        config,
        backend: Literal["json", "mongo"] | None = None,
    ) -> None:
        namespace = inspect.currentframe().f_back.f_globals

        if backend is None and isinstance(config, dict):
            raise ValueError("Cannot determine client type from backend and config")
        elif backend == "json" or check_config(config, JSONConfig):
            cls._client = JSONClient(config)
            base_class = JSONCollection
        elif backend == "mongo" or check_config(config, MongoConfig):
            cls._client = MongoClient(config)
            base_class = MongoCollection
        elif backend == "milvus" and check_config(config, MilvusConfig):
            cls._client = MilvusClient(config)
            base_class = MilvusCollection
        else:
            raise ValueError(f"Backend not found for config type: {type(config)!r}.")

        RedB.process_subclasses(base_class, namespace)
        RedB.process_indices(namespace)

    @staticmethod
    def process_subclasses(
        base_class: Type[Collection],
        namespace: dict[str, Any],
    ) -> None:
        for sub_class in RedB._sub_classes:
            new_parents = []
            for idx, parent_class in enumerate(sub_class.__mro__[1:], start=1):
                if parent_class == Document:
                    # Overwrite Document with correct collection class
                    new_parents.append(base_class)

                    # Add remaining parents
                    new_parents.extend(sub_class.__mro__[idx + 1 :])
                    break

                # Add previous parents
                new_parents.append(parent_class)

            attrs = dict(sub_class.__dict__)
            attrs.update(dict(base_class.__dict__))
            RedB._processed_classes.add(sub_class.__name__)

            new_type = type(sub_class.__name__, tuple(new_parents), attrs)

            # Add class specific variables (members, annotations, etc)
            for key, value in inspect.getmembers(sub_class):
                if key in NON_IHERITABLE_METHODS:
                    continue

                if key.startswith("__") and key.endswith("__"):
                    setattr(new_type, key, value)

            namespace[sub_class.__name__] = new_type

    @staticmethod
    def process_indices(
        namespace: dict[str, Any],
    ) -> None:
        for clsname, grouped_indices in RedB._indices.items():
            current_class: Collection = namespace[clsname]

            for group_name in grouped_indices:
                indices = grouped_indices[group_name]

                full_name = [0] * (len(indices) + 1)
                full_name[0] = indices[0].group_name

                unique = False
                directions = []
                for idx, indice in enumerate(indices, start=1):
                    full_name[indice.order or idx] = indice.name
                    unique = unique or indice.unique
                    directions.append(indice.direction or Direction.ASCENDING)

                current_class.create_indice(
                    Index(
                        names=full_name,
                        unique=unique,
                        directions=directions,
                    )
                )


class DocumentMetaclass(ModelMetaclass):
    def __new__(
        cls: Type[Collection],
        clsname: str,
        bases: list[Type[Collection]],
        attrs: dict[str, Any],
    ):
        class_type = super().__new__(cls, clsname, bases, attrs)
        if RedB._sub_classes is None:
            RedB._sub_classes = []

        if clsname not in processed_classes:
            RedB._sub_classes.append(class_type)

        return class_type


class Document(Collection, metaclass=DocumentMetaclass):
    id: str = Field(default_factory=lambda: str(ObjectId()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow
    )  # TODO: autoupdate this field on updates
