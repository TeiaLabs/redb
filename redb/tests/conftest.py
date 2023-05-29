import os
from pathlib import Path

import dotenv
import pytest

from redb.core import RedB
from redb.interface.configs import JSONConfig, MongoConfig

dotenv.load_dotenv()


@pytest.fixture(scope="session")
def client_path():
    return Path("./tmp/")


@pytest.fixture(scope="session")
def db_path(client_path):
    return client_path / "resources"


@pytest.fixture(scope="session")
def collection_path(db_path):
    return db_path / "embedding"


@pytest.fixture()
def json_client(client_path):
    RedB.setup(
        JSONConfig(
            client_folder_path=client_path,
            default_database_folder_path="resources",
        )
    )


@pytest.fixture(autouse=True)
def mongo_client():
    RedB.setup(
        MongoConfig(
            database_uri=os.environ["MONGODB_URI"],
        )
    )
