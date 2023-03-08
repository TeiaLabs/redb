from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Optional

import pandas as pd
from melting_face.completion.openai_model import get_tokenizer
from melting_face.encoders import EncoderClient, LocalSettings, RemoteSettings
from redb.core import RedB
from redb.interface.configs import CONFIG_TYPE, JSONConfig, MigoConfig, MongoConfig
from redb.teia_schema import Embedding, File, Instance


@dataclasses.dataclass
class KBFilterSettings:
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
    threshold: Optional[float] = 0.5
    top_k: Optional[int] = 10

    def __post_init__(self):
        if (self.threshold < 0.0) or (self.threshold > 1.0):
            raise ValueError("Threshold must be in [0, 1].")
        if self.top_k <= 0:
            raise ValueError("Top K must be greater than 0.")


@dataclasses.dataclass
class KBManagerSettings:
    """
    Knowledge base manager settings.
    """

    redb_config: JSONConfig | MigoConfig | MongoConfig
    model_config: LocalSettings | RemoteSettings
    search_settings: list[KBFilterSettings]


class KnowledgeBaseManager:
    """
    Knowledge base manager for Teia Schema documents.

    Currently supports the following operations:
        - Insert/Update `Instance` embeddings using a predefined model.
        - Search for most similar `Instance`s using filters (`KBFilterSetting`).
        - Count number of `Instance`s in entire database or knowledge base.

    Args:
        redb_config: ReDB configuration file.
        model_config: encoder model configuration file.
        search_settings: default search settings to use when querying knowledge
            bases. Each filter in the list corresponds to one knowledge that
            exists in the database.
    """

    def __init__(
        self,
        redb_config: CONFIG_TYPE,
        model_config: LocalSettings | RemoteSettings,
        search_settings: list[KBFilterSettings],
    ) -> None:
        RedB.setup(redb_config)
        self.redb_config = redb_config
        self.model_config = model_config
        self.encoder = EncoderClient(model_config)
        self.encoder_tokenizer = get_tokenizer(model_config.model_kwargs["model_name"])
        self.memory_search = not isinstance(redb_config, MigoConfig)  # JSON, Mongo
        self.mongo_backend = not isinstance(redb_config, JSONConfig)  # Mongo, Migo
        self._validate_search_settings(search_settings)
        self.search_settings = search_settings
        self.refresh_local_kb()

    @classmethod
    def from_settings(cls, settings: KBManagerSettings) -> KnowledgeBaseManager:
        return cls(
            redb_config=settings.redb_config,
            model_config=settings.model_config,
            search_settings=settings.search_settings,
        )

    def update_instance_embeddings(
        self,
        kb_name: str | None = None,
        overwrite: bool = False,
        update_local_kb: bool = False,
    ) -> list[str]:
        """
        Insert or update embeddings for `Instance`s based on current model.
        If `kb_name` is `None`, updates embeddings for all knowledge bases.

        Args:
            kb_name: knowledge base name.
            overwrite: whether to overwrite all instances or not.
            update_local_kb: whether to update the local knowledge base with
                new instances (only needed for Mongo and JSON backends). If
                you prefer to delay updating the local replica, you may call
                `self.refresh_local_kb` at a later moment.

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
        updated_ids = []
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
            updated_ids.append(instance.id)

        if update_local_kb:
            # update local kb with new instances
            self.refresh_local_kb()

        return updated_ids

    def search_instances(
        self,
        query: str,
        search_settings: list[KBFilterSettings] | None = None,
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
        search_func = (
            self._search_instances_memory
            if self.memory_search
            else self._search_instances_db
        )
        return search_func(query, search_settings)

    def refresh_local_kb(self, force_refresh: bool = False) -> None:
        """
        Refreshes the local replica of the knowledge base, which is used to
        cache searches in DBs that are inefficient at vector search. If you
        are using a ReDB configuration that requires in-memory searches and
        forget to call this method after operations that modify the database,
        the local replica may be outdated and results will reflect that.

        The local knowledge base replica consists only of instances that have
        precomputed content and query embeddings.

        Args:
            force_refresh: forces a full refresh of the local database replica.
        """
        # if we are using a backend that can perform searches, ignore refreshes
        if not self.memory_search:
            return None

        # if local replica does not exist, create it (empty at first)
        if not hasattr(self, "local_kb") or force_refresh:
            columns = [
                field_model.alias if field_model.alias else field_name
                for field_name, field_model in Instance.__fields__.items()
            ]
            self.local_kb = pd.DataFrame([], columns=columns)

        # generate diff and populate with missing instances
        model_type = self.model_config.model_type
        model_name = self.model_config.model_kwargs["model_name"]
        # get local instances' IDs and last update timestamp
        instances_local = self.local_kb[["_id", "updated_at"]].to_dict(orient="records")
        # get DB instances' IDs and last update timestamp
        instances_db = Instance.find_many(
            filter={
                "content_embedding.model_type": model_type,
                "content_embedding.model_name": model_name,
            },
            fields=["_id", "updated_at"],
        )

        # remove local instances that do not exist in DB (based on IDs)
        ids_local = [i["_id"] for i in instances_local]
        ids_db = [i["_id"] for i in instances_db]
        ids_remove_local = list(set(ids_local).difference(ids_db))
        if ids_remove_local:
            self.local_kb.drop(
                labels=self.local_kb["_id"].isin(ids_remove_local).index,
                axis="index",
                inplace=True,
            )
            self.local_kb.reset_index(drop=True, inplace=True)

        # add DB instances that are missing in local replica (IDs + update)
        instances_local = self.local_kb[["_id", "updated_at"]].to_dict(orient="records")
        map_id_update_local = {i["_id"]: i["updated_at"] for i in instances_local}
        map_id_update_db = {i["_id"]: i["updated_at"] for i in instances_db}
        new_ids = [
            db_id
            for db_id, _ in map_id_update_db.items()
            if db_id not in set(map_id_update_local.keys())
        ]
        was_updated = [
            local_id
            for local_id, local_update in map_id_update_local.items()
            if local_id in set(map_id_update_db.keys())
            and datetime.fromisoformat(local_update) < map_id_update_db[local_id]
        ]
        if was_updated:
            self.local_kb.drop(
                labels=self.local_kb["_id"].isin(was_updated).index,
                axis="index",
                inplace=True,
            )
            self.local_kb.reset_index(drop=True, inplace=True)

        ids_missing = list(set(new_ids + was_updated))
        if ids_missing:
            instances_missing = Instance.find_many({"_id": {"$in": ids_missing}})
            instances_missing_df = Instance.instances_to_dataframe(
                instances_missing, explode_vectors=True
            )
            self.local_kb = pd.concat(
                [self.local_kb, instances_missing_df], ignore_index=True
            )

    def __len__(self) -> int:
        """Returns the length of the managed database."""
        if self.memory_search:
            return len(self.local_kb)
        else:
            raise NotImplementedError

    def get_kb_length(self, kb_name: str) -> int:
        """
        Returns the length of a managed knowledge base.

        Args:
            kb_name: knowledge base name.

        Returns:
            int: length of knowledge base.
        """
        kb_names = Instance.get_kb_names()
        if kb_name not in kb_names:
            raise ValueError(
                f"Invalid knowledge base name: '{kb_name}'. "
                f"Knowledge bases found: {kb_names}."
            )
        return Instance.count_documents(filter={"kb_name": kb_name})

    def search_result_to_string(
        self,
        search_result: pd.DataFrame,
        to_relevance: bool = True,
        max_chars_line: int = 512,
        max_total_tokens: int = 1024,
    ) -> str:
        """
        Transforms a `pd.DataFrame` containing search results into a string.

        Args:
            search_result: A DataFrame containing search results from the
                database (result of `self.search_instances`).
            to_relevance: whether to return similarity results (i.e., distances
                in [0, 1]) in relevance format (i.e., percentages in [0, 100]).
            max_chars_line: maximum number of *characters* per line.
            max_total_tokens: maximum number of total *tokens* for final result.

        Returns:
            str: stringified search results.
        """
        # check if dataframe has all required columns
        required_cols = ["content", "kb_name", "distances"]
        if not set(required_cols).issubset(search_result.columns):
            raise ValueError("Invalid search results to stringify (missing columns).")

        # compute strings for each line while counting number of tokens
        result = []
        total_tokens = 0
        for _, row in search_result.iterrows():
            score = row["distances"]
            if to_relevance:
                score = (1 - score) * 100
            line = (
                f"Data: {row['content'][:max_chars_line]}\n"
                f"Source: {row['kb_name']}\n"
                f"Relevance: {score:2.1f}\n"
            )
            num_tokens = self.encoder_tokenizer.count_tokens(line)
            if (total_tokens + num_tokens) > max_total_tokens:
                break
            result.append(line)
            total_tokens += num_tokens

        # generate final string
        result_final = "\n".join(result)
        return result_final

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
                update={
                    f"{embedding_name}": embedding.dict(),
                },
                operator="$push",
            )
        else:
            # found instance with these embedding parameters, update it
            mongo_driver = Instance._get_driver_collection(Instance)
            res = mongo_driver.update_one(
                filter={"_id": instance.id},
                update={
                    "$set": {f"{embedding_name}.$[embedding].vector": embedding.vector},
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
        Instance.update_one(
            filter={"_id": instance.id}, update={"updated_at": datetime.utcnow()}
        )

    def _update_instance_embedding_memory(
        self,
        instance: Instance,
        embedding: Embedding,
        embedding_name: str,
    ):
        raise NotImplementedError

    def _validate_search_settings(
        self, search_settings: list[KBFilterSettings]
    ) -> None:
        """
        Validates a list of knowledge base search settings to guarantee that
        the requested knowledge bases exist.

        Args:
            search_settings: list of knowledge base search settings.
        """
        # if there are no documents, skip validations
        num_instances = Instance.count_documents()
        if num_instances == 0:
            return

        # check if knowledge bases exist
        kb_names = Instance.get_kb_names()
        for settings in search_settings:
            if settings.kb_name not in kb_names:
                raise ValueError(f"Invalid knowledge base name: {settings.kb_name}.")

    def _search_instances_memory(
        self, query: str, search_settings: list[KBFilterSettings]
    ) -> pd.DataFrame:
        if "vector" not in self.local_kb:
            columns = [
                field_model.alias if field_model.alias else field_name
                for field_name, field_model in Instance.__fields__.items()
            ]
            return pd.DataFrame([], columns=columns)

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

    def _search_instances_db(
        self,
        query: str,
        search_settings: list[KBFilterSettings],
    ) -> pd.DataFrame:
        raise NotImplementedError


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
    curr_time = datetime.utcnow()
    file = File(
        scraped_at=curr_time,
        last_modified_at=curr_time,
        hash="12345",
        organization_id="TeiaLabs",
        size_bytes=100,
        url_original="file://tmp/main.py",
    )
    try:
        File.insert_one(file)
    except Exception as e:
        pass

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
            # content_embedding=[],
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
        try:
            Instance.insert_one(instance)
        except Exception as e:
            # print(e)
            print("UPDATING DATES")
            Instance.update_one(
                filter={"_id": instance.id},
                update={"updated_at": datetime.now()},
            )


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
        KBFilterSettings(kb_name="documents", threshold=1.0, top_k=3),
        KBFilterSettings(kb_name="sfcc", threshold=1.0, top_k=2),
    ]

    RedB.setup(redb_config)
    Instance.delete_many({})
    add_test_documents()
    kb_manager = KnowledgeBaseManager(
        redb_config=redb_config,
        model_config=model_config,
        search_settings=search_settings,
    )
    print(kb_manager.local_kb)

    updated_ids = kb_manager.update_instance_embeddings(overwrite=True)
    kb_manager.refresh_local_kb()
    print(kb_manager.local_kb)
    search_result = kb_manager.search_instances(query="OSF Organization")
    print(search_result["content"])
    print("---")

    search_results_str = kb_manager.search_result_to_string(
        search_result=search_result,
        to_relevance=True,
        max_chars_line=512,
        max_total_tokens=1024,
    )
    print(search_results_str)
