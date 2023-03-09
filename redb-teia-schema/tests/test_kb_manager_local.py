import pytest

from redb.teia_schema import Instance
from redb.teia_schema.knowledge_base import (
    KnowledgeBaseManager,
    KBFilterSettings,
    KBManagerSettings,
)
from melting_face.encoders import LocalSettings
from redb.core import RedB
from redb.interface.configs import JSONConfig, MongoConfig


class TestKBManagerLocal:
    @pytest.fixture(scope="session")
    def mongo_config(self) -> MongoConfig:
        cfg = MongoConfig(
            database_uri="mongodb://localhost:27017",
            default_database="test_db",
        )
        RedB.setup(cfg)
        return cfg

    @pytest.fixture(scope="session")
    def json_config(self) -> JSONConfig:
        cfg = JSONConfig(
            client_folder_path="/tmp/redb",
            default_database_folder_path="test_db",
        )
        RedB.setup(cfg)
        return cfg

    @pytest.fixture(scope="session")
    def model_config(self) -> LocalSettings:
        model_config = LocalSettings(
            model_type="sentence_transformer",
            model_kwargs=dict(
                model_name="paraphrase-albert-small-v2",
                device="cpu",
            ),
        )
        return model_config

    @pytest.fixture(scope="session")
    def search_settings(self) -> list[KBFilterSettings]:
        search_settings = [
            KBFilterSettings(kb_name="documents", threshold=1.0, top_k=3),
            KBFilterSettings(kb_name="sfcc", threshold=1.0, top_k=2),
        ]
        return search_settings

    @pytest.fixture(scope="session")
    def kb_manager_mongo(
        self,
        mongo_config: MongoConfig,
        model_config: LocalSettings,
        search_settings: list[KBFilterSettings],
    ) -> KnowledgeBaseManager:
        kb_manager = KnowledgeBaseManager(
            redb_config=mongo_config,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=True,
        )
        return kb_manager

    @pytest.fixture(scope="session")
    def kb_manager_json(
        self,
        json_config: JSONConfig,
        model_config: LocalSettings,
        search_settings: list[KBFilterSettings],
    ) -> KnowledgeBaseManager:
        kb_manager = KnowledgeBaseManager(
            redb_config=json_config,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=True,
        )
        return kb_manager

    def test_search_settings(self):
        _ = KBFilterSettings(kb_name="abc")
        with pytest.raises(ValueError):
            # invalid thresholds
            _ = KBFilterSettings(kb_name="abc", threshold=-1.0)
            _ = KBFilterSettings(kb_name="abc", threshold=2.0)
            # invalid top_k
            _ = KBFilterSettings(kb_name="abc", top_k=-1)

    def test_mongo_driver(self, kb_manager_mongo):
        """KB manager instantiates with a valid ReDB MongoDB driver."""
        assert isinstance(kb_manager_mongo, KnowledgeBaseManager)

    def test_json_driver(self, kb_manager_json):
        """KB manager instantiates with a valid ReDB JSON driver."""
        assert isinstance(kb_manager_json, KnowledgeBaseManager)

    def test_from_settings(self, json_config, model_config, search_settings):
        """KB manager instantiation using from_settings."""
        settings = KBManagerSettings(
            redb_config=json_config,
            model_config=model_config,
            search_settings=search_settings,
        )
        kb_manager = KnowledgeBaseManager.from_settings(settings)
        assert isinstance(kb_manager, KnowledgeBaseManager)

    def test_kb_manager_invalid_search_settings(self, json_config, model_config):
        """Search settings validation (invalid KB name)."""
        search_settings = KBFilterSettings(kb_name="")
        with pytest.raises(ValueError):
            _ = KnowledgeBaseManager(
                redb_config=json_config,
                model_config=model_config,
                search_settings=[search_settings],
            )

    def test_preload_local_kb(self, mongo_config, model_config, search_settings):
        """Local database replica preloading."""
        # without preload
        kb_manager = KnowledgeBaseManager(
            redb_config=mongo_config,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=False,
        )
        assert kb_manager.local_kb is not None
        assert len(kb_manager.local_kb) == 0

        # with preload
        kb_manager = KnowledgeBaseManager(
            redb_config=mongo_config,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=True,
        )
        assert kb_manager.local_kb is not None
        assert len(kb_manager.local_kb) != 0

    def test_kb_length(self, kb_manager_json):
        """Local database length for a specific KB name."""
        assert kb_manager_json.get_kb_length("documents") == 2
        assert kb_manager_json.get_kb_length("sfcc") == 1

    @pytest.mark.skip("Need to improve test structure first")
    def test_refresh_local_kb(self):
        """Local database replica refreshing."""
        # instantiate KB manager with preload flag as True
        # operate on DB externally (e.g., insert instances)
        # call method to refresh local KB
        # check if local KB has new instances
        pass

    @pytest.mark.skip("Need to improve test structure first")
    def test_search_local(self):
        """Search results."""
        # instantiate KB manager with preload flag as True
        # search for instances in a specific KB
        # check search results properties
        pass

    @pytest.mark.skip("Need to improve test structure first")
    def test_update_embeddings(self):
        """Update embeddings for all instances."""
        # instantiate KB manager with preload flag as True
        # update instance embeddings for a specific KB
        # check local KB properties

        # update instance embeddings for all KBs
        # check local KB properties

        # update all instance embeddings (overwrite)
        # check local KB properties
        pass
