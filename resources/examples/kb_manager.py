import logging
from datetime import datetime

from melting_face.encoders import LocalSettings
from redb.core import RedB, MongoConfig
from redb.teia_schema import Embedding, File, Instance
from redb.teia_schema.knowledge_base import (
    KnowledgeBaseManager,
    KBFilterSettings,
    KBManagerSettings,
)


def add_test_documents():
    Instance.delete_many({})
    File.delete_many({})

    curr_time = datetime.utcnow()
    file = File(
        scraped_at=curr_time,
        last_modified_at=curr_time,
        hash="12345",
        organization_id="TeiaLabs",
        size_bytes=100,
        url_original="file://tmp/main.py",
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
            kb_name="osf_docs",
        ),
        dict(
            query="OSF people",
            content="CEO: Gerry.\nVP of Innovation: Simion.",
            kb_name="osf_docs",
        ),
        dict(
            query="SFCC Guidelines",
            content="Just give up.",
            kb_name="sfcc",
        ),
        dict(
            query=None,
            content="I have no query!!!.",
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
        Instance.insert_one(instance)


def main():
    # the knowledge base manager logs a lot of stuff to help debug
    logging.basicConfig(level=logging.DEBUG)

    # first, lets create a RedB configuration and populate it with test data
    test_db_name = "test_db"
    redb_config = MongoConfig(
        database_uri="mongodb://localhost:27017",
        default_database=test_db_name,
    )
    RedB.setup(redb_config)  # you MUST call setup before using the KB manager
    add_test_documents()

    # now, let's create the other KB manager settings
    # model used to encode and search for instances
    model_config = LocalSettings(
        model_type="sentence_transformer",
        model_kwargs=dict(
            model_name="paraphrase-albert-small-v2",
            device="cpu",
        ),
    )
    # search settings per sub-knowledge base
    search_settings = [
        KBFilterSettings(kb_name="osf_docs", threshold=1.0, top_k=3),
        KBFilterSettings(kb_name="sfcc", threshold=1.0, top_k=2),
    ]

    # create KB manager using settings declared above
    # you may also call KnowledgeBaseManager.from_settings (dict-based)
    kb_manager = KnowledgeBaseManager(
        database_name=test_db_name,
        model_config=model_config,
        search_settings=search_settings,
        preload_local_kb=True,
    )
    # since we preloaded the local KB replica, we can print its contents
    print(kb_manager.local_kb)

    # we can print the number of documents in a KB and the name of sub-kbs
    print(len(kb_manager))  # total number of documents
    print(kb_manager.get_kb_names())  # sub-kb names
    print(kb_manager.get_kb_length(kb_name="osf_docs"))  # osf_docs sub-kb

    # we can update embeddings for Instances managed by the KB manager
    # remember to check the documentation to see other parameters
    updated_ids = kb_manager.update_instance_embeddings(
        kb_name=None,  # updates all sub-knowledge bases ("osf_docs", "sfcc")
        overwrite=True,
        update_local_kb=False,  # local replica will be outdated (call refresh)
        batch_size=2,
    )
    # refresh outdated local replica
    kb_manager.refresh_local_kb()
    print(kb_manager.local_kb)

    # search for instances in the database
    query = "OSF Organization"
    search_result = kb_manager.search_instances(query=query, match_with="content")
    print(f"Query: {query}")
    print(search_result[["content", "distances"]])
    print("---")

    # we can also stringify search results (useful for prompt engineering)
    search_results_str = kb_manager.search_result_to_string(
        search_result=search_result,
        to_relevance=True,
        max_chars_line=512,
        max_total_tokens=1024,
    )
    print(search_results_str)


if __name__ == "__main__":
    main()
