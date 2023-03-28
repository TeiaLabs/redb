from datetime import datetime

import pytest
from redb.teia_schema import Embedding, File, Instance
from redb.teia_schema.knowledge_base import (
    KnowledgeBaseManager,
    KBFilterSettings,
    KBManagerSettings,
)
from melting_face.encoders import LocalSettings
from redb.core import RedB
from redb.interface.configs import MongoConfig


class TestKBManagerLocal:

    @pytest.fixture
    def test_docs(self):
        # pre
        Instance.delete_many({})
        File.delete_many({})
        curr_time = datetime.utcnow()
        file = File(
            scraped_at=curr_time,
            last_modified_at=curr_time,
            hash="12345",
            organization_id="TeiaLabs",
            size_bytes=100,
            url_original="file://tmp/doc.txt",
        )
        File.insert_one(file)

        embedding = Embedding(
            model_type="sentence_transformer",
            model_name="paraphrase-albert-small-v2",
            vector=[1.0, 2.0, 3.0, 4.0, 5.0],
        )

        data = [
            dict(
                query="What to do when you find a dog on the street",
                content="You should always pet the dog.",
                kb_name="documents",
            ),
            dict(
                query="OSF people",
                content="CEO: Gerry.\nVP of Innovation: Simion.",
                kb_name="documents",
            ),
            dict(
                query="SFCC Guidelines",
                content="Just give up.",
                kb_name="sfcc",
            ),
        ]

        for i, d in enumerate(data):
            instance = Instance(
                content_embedding=[embedding],
                content=d["content"],
                data_type="text",
                file_id=file.id,
                kb_name=d["kb_name"],
                query=d["query"],
                query_embedding=[],
                url=f"file://tmp/document{i}.txt",
            )
            Instance.insert_one(instance)

        # post
        yield
        Instance.delete_many({})
        File.delete_many({})

    @pytest.fixture
    def mongo_config(self) -> MongoConfig:
        cfg = MongoConfig(
            database_uri="mongodb://localhost:27017",
            default_database="test_db",
        )
        RedB.setup(cfg)
        return cfg

    @pytest.fixture
    def model_config(self) -> LocalSettings:
        model_config = LocalSettings(
            model_type="sentence_transformer",
            model_kwargs=dict(
                model_name="paraphrase-albert-small-v2",
                device="cpu",
            ),
        )
        return model_config

    @pytest.fixture
    def search_settings(self) -> list[KBFilterSettings]:
        search_settings = [
            KBFilterSettings(kb_name="documents", threshold=1.0, top_k=3),
            KBFilterSettings(kb_name="sfcc", threshold=1.0, top_k=2),
        ]
        return search_settings

    @pytest.fixture
    def kb_manager_mongo(
        self,
        mongo_config: MongoConfig,
        model_config: LocalSettings,
        search_settings: list[KBFilterSettings],
    ) -> KnowledgeBaseManager:
        kb_manager = KnowledgeBaseManager(
            database_name=mongo_config.default_database,
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

    def test_from_settings(self, mongo_config, model_config, search_settings):
        """KB manager instantiation using from_settings."""
        settings = KBManagerSettings(
            database_name=mongo_config.default_database,
            redb_config=mongo_config,
            model_config=model_config,
            search_settings=search_settings,
        )
        kb_manager = KnowledgeBaseManager.from_settings(settings)
        assert isinstance(kb_manager, KnowledgeBaseManager)

    @pytest.mark.usefixtures("test_docs")
    def test_kb_manager_invalid_search_settings(self, mongo_config, model_config):
        """Search settings validation (invalid KB name)."""
        search_settings = KBFilterSettings(kb_name="abc")
        with pytest.raises(ValueError):
            _ = KnowledgeBaseManager(
                database_name=mongo_config.default_database,
                model_config=model_config,
                search_settings=[search_settings],
            )

    @pytest.mark.usefixtures("test_docs")
    def test_preload_local_kb(self, mongo_config, model_config, search_settings):
        """Local database replica preloading."""
        # without preload
        kb_manager = KnowledgeBaseManager(
            database_name=mongo_config.default_database,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=False,
        )
        assert kb_manager.local_kb is not None
        assert len(kb_manager.local_kb) == 0

        # with preload
        kb_manager = KnowledgeBaseManager(
            database_name=mongo_config.default_database,
            model_config=model_config,
            search_settings=search_settings,
            preload_local_kb=True,
        )
        assert kb_manager.local_kb is not None
        assert len(kb_manager.local_kb) == 3

    @pytest.mark.usefixtures("test_docs")
    def test_kb_length(self, kb_manager_mongo: KnowledgeBaseManager):
        """Local database length for a specific KB name."""
        assert kb_manager_mongo.get_kb_length("documents") == 2
        assert kb_manager_mongo.get_kb_length("sfcc") == 1

    @pytest.mark.usefixtures("test_docs")
    def test_refresh_local_kb(self, kb_manager_mongo: KnowledgeBaseManager):
        """Local database replica refreshing."""
        kb_manager_mongo.refresh_local_kb()
        assert len(kb_manager_mongo.local_kb) == 3

        instance = Instance(
                content="New document!",
                data_type="text",
                kb_name="sfcc",
                query="New query!",
            )
        Instance.insert_one(instance)
        kb_manager_mongo.refresh_local_kb()
        assert len(kb_manager_mongo.local_kb) == 3

    @pytest.mark.usefixtures("test_docs")
    def test_update_embeddings(self, kb_manager_mongo: KnowledgeBaseManager):
        """Update embeddings for all instances."""
        kb_manager_mongo.refresh_local_kb()
        local_kb_old = kb_manager_mongo.local_kb
        sfcc_data = local_kb_old[local_kb_old["kb_name"] == "sfcc"]
        vector_len_old = len(sfcc_data["content_embedding"].iloc[0])
        assert vector_len_old == 5

        # update for a single KB
        kb_manager_mongo.update_instance_embeddings(
            kb_name="sfcc",
            update_local_kb=True,
        )
        local_kb = kb_manager_mongo.local_kb
        sfcc_data = local_kb[local_kb["kb_name"] == "sfcc"]
        vector_len = len(sfcc_data["content_embedding"].iloc[0])
        update_timestamp1 = sfcc_data["updated_at"].iloc[0]
        assert vector_len > vector_len_old
        # ...and test if other KB's vectors are still the same
        other_data = local_kb[local_kb["kb_name"] == "documents"]
        vector_lengths = [len(a) for a in other_data["content_embedding"]]
        assert all([vl == 5 for vl in vector_lengths])

        # update all KBs (without overwrite)
        kb_manager_mongo.update_instance_embeddings(
            update_local_kb=True,
        )
        local_kb = kb_manager_mongo.local_kb
        sfcc_data = local_kb[local_kb["kb_name"] == "sfcc"]
        update_timestamp2 = sfcc_data["updated_at"].iloc[0]
        assert update_timestamp1 == update_timestamp2
        other_data = local_kb[local_kb["kb_name"] == "documents"]
        vector_lengths = [len(a) for a in other_data["content_embedding"]]
        assert all([vl > 5 for vl in vector_lengths])

    @pytest.mark.usefixtures("test_docs")
    def test_search_local(self, kb_manager_mongo: KnowledgeBaseManager):
        """Search results."""
        kb_manager_mongo.update_instance_embeddings(
            overwrite=True,
            update_local_kb=True,
        )

        # invalid search settings
        invalid_search_settings = KBFilterSettings(kb_name="invalid")
        with pytest.raises(ValueError):
            _ = kb_manager_mongo.search_instances(
                query="Whatever",
                search_settings=[invalid_search_settings],
            )

        results = kb_manager_mongo.search_instances("OSF Organization")
        assert len(results) > 0

