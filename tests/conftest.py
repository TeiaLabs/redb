from pathlib import Path

import pytest

from redb import RedB
from redb.json_system import JSONConfig


@pytest.fixture(scope="session")
def client_path():
    return Path("./tmp/")


@pytest.fixture(scope="session")
def db_path(client_path):
    return client_path / "resources"


@pytest.fixture(scope="session")
def collection_path(db_path):
    return db_path / "Embedding"


@pytest.fixture(autouse=True, scope="session")
def client(client_path, db_path):
    RedB.setup(JSONConfig(client_folder_path=client_path, default_database_folder_path=db_path))
