import dataclasses

import pandas as pd
from melting_face.encoders import EncoderClient, LocalSettings, RemoteSettings
from redb.core import RedB
from redb.interface.configs import CONFIG_TYPE, JSONConfig, MigoConfig, MongoConfig
from redb.teia_schema import Embedding, File, Instance


# Define and implement the minimal interfaces for a Knowledge Bases manager so we can remove the athena-indexer package:

# Manager for Knowledge Bases must:
#     insert instances (batch) to a kb
#     update/extract embeddings
#     load knowledge bases from disk
#     perform search loading from database (memory search)

# Overall, KB Manager is the class that uses an encoder from melting-face (defined by model_name and model_type) and teia-schema from redb.
# Probably, in a near future we should have a search service running to support all projects.

# Basically we have to unify the athena-bot/kb.py and the athena-indexer/extract scripts.

# In addition, we must design an interface that is compatible with loading from disk for memory search or doing efficient database-powered vector search (milvus/migo).


@dataclasses.dataclass
class KBFilterSetting:
    """
    Knowledge base filter settings.

    Args:
        kb_name: knowledge base name.
        threshold: similarity threshold. Interval: [0, 1].
        top_k: maximum number of instances to retrieve. If the number of
            instances over the `threshold` are less than `top_k`, the
            result will contain fewer instances than `top_k`.
    """

    kb_name: str
    threshold: float = 0.5
    top_k: int = 10

    def __post_init__(self):
        if (self.threshold < 0.0) or (self.threshold > 1.0):
            raise ValueError("Threshold must be in [0, 1].")
        if self.top_k <= 0:
            raise ValueError("Top K must be greater than 0.")


class KnowledgeBaseManager:
    """
    Knowledge base manager for Teia Schema documents.

    Args:
        redb_config: ReDB configuration file.
        model_config: encoder model configuration file.
    """

    def __init__(
        self,
        redb_config: CONFIG_TYPE,
        model_config: LocalSettings | RemoteSettings,
        search_settings: list[KBFilterSetting],
    ) -> None:
        RedB.setup(redb_config)
        self.redb_config = redb_config
        self.model_config = model_config
        self.encoder = EncoderClient(model_config)
        self.memory_search = not isinstance(redb_config, MigoConfig)
        self.mongo_backend = not isinstance(redb_config, JSONConfig)
        self._validate_search_settings(search_settings)
        self.search_settings = search_settings

    def update_embeddings(
        self,
        kb_name: str | None = None,
        overwrite: bool = False,
    ) -> list[str]:
        """
        Insert or update embeddings for `Instance`s based on current model.
        If `kb_name` is `None`, updates embeddings for all knowledge bases.

        Args:
            kb_name: knowledge base name.
            overwrite: whether to overwrite all instances or not.

        Returns:
            list[str]: list of IDs for updated instances.
        """
        model_type = self.model_config.model_type
        model_name = self.model_config.model_kwargs["model_name"]
        # find all relevant instances
        kb_filter = {"kb_name": kb_name} if kb_name else {}
        instances = Instance.find_many(filter=kb_filter, fields=["_id"])
        if not instances:
            return []

        if overwrite:
            ids_missing = [i["_id"] for i in instances]
        else:
            # find all instances without embeddings for our model
            model_filters = {
                "content_embedding.model_type": model_type,
                "content_embedding.model_name": model_name,
                "query_embedding.model_type": model_type,
                "query_embedding.model_name": model_name,
            }
            model_filters.update(kb_filter)
            instances_filled = Instance.find_many(model_filters, fields=["_id"])
            ids_missing = list(
                set([i["_id"] for i in instances]).difference(
                    [i["_id"] for i in instances_filled]
                )
            )
            if not ids_missing:
                return []

        # update all instances that have missing embeddings for our model
        if self.mongo_backend:
            update_func = self._update_instance_embedding_mongo
        else:
            update_func = self._update_instance_embedding_memory
        instances_missing = Instance.find_many({"_id": {"$in": ids_missing}})
        instances_ids = []
        for instance in instances_missing:
            embs = self.encoder.encode_text(
                texts=[instance.content, instance.query],
                return_type="list",
            )
            content_embedding = Embedding(
                model_name=model_name,
                model_type=model_type,
                vector=embs[0],
            )
            query_embedding = Embedding(
                model_name=model_name,
                model_type=model_type,
                vector=embs[1],
            )
            update_func(instance, content_embedding, "content_embedding")
            update_func(instance, query_embedding, "query_embedding")
            instances_ids.append(instance.id)
        return instances_ids

    def _update_instance_embedding_mongo(
        self,
        instance: Instance,
        embedding: Embedding,
        embedding_name: str,
    ):
        instances_with_emb = Instance.find_many(
            filter={
                "_id": instance.id,
                f"{embedding_name}.model_type": embedding.model_type,
                f"{embedding_name}.model_name": embedding.model_name,
            },
        )
        if not instances_with_emb:
            # did not find instance with these embedding parameters
            Instance.update_one(
                filter={"_id": instance.id},
                update={f"{embedding_name}": embedding.dict()},
                operator="$push",
            )
        else:
            # found instance with these embedding parameters, update it
            mongo_driver = Instance._get_driver_collection(Instance)
            res = mongo_driver.update_one(
                filter={"_id": instance.id},
                update={
                    "$set": {f"{embedding_name}.$[embedding].vector": embedding.vector}
                },
                array_filters=[
                    {
                        "$and": [
                            {"embedding.model_name": {"$eq": embedding.model_name}},
                            {"embedding.model_type": {"$eq": embedding.model_type}},
                        ]
                    }
                ],
            )

    def _update_instance_embedding_memory(
        self,
        instance: Instance,
        embedding: Embedding,
        embedding_name: str,
    ):
        raise NotImplementedError

    def search(
        self,
        query: str,
        search_settings: list[KBFilterSetting] | None = None,
    ) -> pd.DataFrame:
        """
        Search knowledge bases for entries that are most similar to a given query.

        Args:
            query: the query to compare knowledge base entries with.
            search_settings: list of knowledge base search settings. If `None`,
                will default to class search settings.

        Returns:
            pd.DataFrame: search results for all knowledge bases.
        """
        if not search_settings:
            search_settings = self.search_settings
        else:
            self._validate_search_settings(search_settings)
        search_func = self._search_memory if self.memory_search else self._search_db
        return search_func(query, search_settings)

    def _search_memory(
        self, query: str, search_settings: list[KBFilterSetting]
    ) -> pd.DataFrame:
        kb_list = [settings.kb_name for settings in search_settings]
        kb_filter = {"kb_name": {"$in": kb_list}}
        model_type = self.model_config.model_type
        model_name = self.model_config.model_kwargs["model_name"]
        model_filters = {
            "content_embedding.model_type": model_type,
            "content_embedding.model_name": model_name,
            "query_embedding.model_type": model_type,
            "query_embedding.model_name": model_name,
        }
        model_filters.update(kb_filter)
        instances = Instance.find_many(model_filters)
        instances_df = Instance.instances_to_dataframe(instances, explode_vectors=True)

        query_embedding = self.encoder.encode_text([query], return_type="list")[0]
        instances_df["distances"] = distances_from_embeddings_np(
            query_embedding=query_embedding,
            embeddings=instances_df.vector.tolist(),
        )
        instances_df.sort_values(by="distances", inplace=True)

        df_list = []
        for settings in search_settings:
            current_kb = instances_df.query(f"kb_name == @settings.kb_name")
            current_kb = current_kb[current_kb.distances < settings.threshold]
            current_kb = current_kb.sort_values(by="distances")
            current_kb = current_kb.head(settings.top_k)
            df_list.append(current_kb)

        return pd.concat(df_list)

    def _search_db(
        self,
        query: str,
        search_settings: list[KBFilterSetting],
    ) -> pd.DataFrame:
        raise NotImplementedError

    def _validate_search_settings(self, search_settings: list[KBFilterSetting]) -> None:
        """
        Validates a list of knowledge base search settings to guarantee that
        the requested knowledge bases exist.

        Args:
            search_settings: list of knowledge base search settings.
        """
        kb_names = Instance.get_kb_names()
        for settings in search_settings:
            if settings.kb_name not in kb_names:
                raise ValueError(f"Invalid knowledge base name: {settings.kb_name}.")


def distances_from_embeddings_np(
    query_embedding: list[float],
    embeddings: list[list[float]],
    distance_metric="cosine",
) -> list[list]:
    """Return the distances between a query embedding and a list of embeddings."""
    import numpy as np
    query_embedding = np.array(query_embedding)
    query_embedding = query_embedding / np.linalg.norm(query_embedding)
    embeddings = np.array(embeddings)
    embeddings = embeddings / np.linalg.norm(embeddings, axis=1)[:, np.newaxis]
    distances = np.dot(embeddings, query_embedding)
    distances = 1 - distances
    return distances


def add_test_documents():
    from datetime import datetime

    curr_time = datetime.now()
    file = File(
        scraped_at=curr_time,
        last_modified_at=curr_time,
        hash="12345",
        organization_id="TeiaLabs",
        size_bytes=100,
        url_original="file://tmp/main.py",
    )
    File.update_one({"_id": file.id}, file, upsert=True)

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
            # content_embedding=[embedding],
            content_embedding=[],
            content=d["content"],
            data_type="text",
            file_id=file.id,
            kb_name=d["kb_name"],
            query=d["query"],
            # query_embedding=[embedding] if i < 1 else [],
            # query_embedding=[embedding],
            query_embedding=[],
            url=f"file://tmp/document{i}.txt",
        )
        instance.update_one({"_id": instance.id}, instance, upsert=True)


if __name__ == "__main__":
    redb_config = MongoConfig(
        database_uri="mongodb://localhost:27017",
        default_database="test_db",
    )
    # redb_config = JSONConfig(
    #     client_folder_path="/tmp/redb",
    #     default_database_folder_path="test_db",
    # )
    model_config = LocalSettings(
        model_type="sentence_transformer",
        model_kwargs=dict(
            model_name="paraphrase-albert-small-v2",
            device="cpu",
        ),
    )
    search_settings = [
        KBFilterSetting(kb_name="documents", threshold=0.5, top_k=3),
        KBFilterSetting(kb_name="sfcc", threshold=0.5, top_k=2),
    ]

    kb_manager = KnowledgeBaseManager(
        redb_config=redb_config,
        model_config=model_config,
        search_settings=search_settings,
    )

    add_test_documents()

    updated_ids = kb_manager.update_embeddings(overwrite=True)
    search_results = kb_manager.search(query="OSF Organization")
