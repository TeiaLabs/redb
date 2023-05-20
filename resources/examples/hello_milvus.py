import time

import numpy as np
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)

fmt = "\n=== {:30} ===\n"
search_latency_fmt = "search latency = {:.4f}s"
num_entities, dim = 2000, 768

print(fmt.format("start connecting to Milvus"))
connections.connect("default", host="localhost", port="19530")

has_col = utility.has_collection("hello_milvus")
print(f"Does collection hello_milvus exist in Milvus: {has_col}")

# +-+------------+------------+------------------+------------------------------+
# | | field name | field type | other attributes |       field description      |
# +-+------------+------------+------------------+------------------------------+
# |1|    "pk"    |   VarChar  |  is_primary=True |      "primary field"         |
# | |            |            |   auto_id=False  |                              |
# +-+------------+------------+------------------+------------------------------+
# |2|  "random"  |    Double  |                  |      "a double field"        |
# +-+------------+------------+------------------+------------------------------+
# |3|"embeddings"| FloatVector|     dim=8        |  "float vector with dim 8"   |
# +-+------------+------------+------------------+------------------------------+
fields = [
    FieldSchema(
        name="pk",
        dtype=DataType.VARCHAR,
        is_primary=True,
        auto_id=False,
        max_length=100,
    ),
    FieldSchema(name="random", dtype=DataType.VARCHAR, max_length=100),
    FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dim),
]
schema = CollectionSchema(
    fields, "hello_milvus is the simplest demo to introduce the APIs"
)
hello_milvus = Collection("hello_milvus", schema, consistency_level="Strong")

print(fmt.format("Start inserting entities"))
rng = np.random.default_rng(seed=19530)

import os
import dotenv
from melting_face.encoders import LocalSettings
from knowledge_base import Instance, KnowledgeBaseManager
from redb.core import RedB, MongoConfig
dotenv.load_dotenv()
mongo_config = MongoConfig(os.environ["MONGODB_URI"])
RedB.setup(mongo_config)
sets = LocalSettings(
    model_type="sentence_transformer",
    model_kwargs={
        "model_name": "paraphrase-distilroberta-base-v1",
        "device": "cpu",
    }
)
# exit()
print(fmt.format("Start Creating index IVF_FLAT"))
index = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 128},
}
# create_index() can only be applied to `FloatVector` and `BinaryVector` fields.
print(hello_milvus.has_index())
hello_milvus.create_index("embeddings", index)
print(hello_milvus.has_index())
################################################################################
# 5. search, query, and hybrid search
# After data were inserted into Milvus and indexed, you can perform:
# - search based on vector similarity
# - query based on scalar filtering(boolean, int, etc.)
# - hybrid search based on vector similarity and scalar filtering.
#

# Before conducting a search or a query, you need to load the data in `hello_milvus` into memory.
print(fmt.format("Start loading"))
hello_milvus.load()

# -----------------------------------------------------------------------------
# search based on vector similarity
print(fmt.format("Start searching based on vector similarity"))
vectors_to_search = entities[-1][-2:]
search_params = {
    "metric_type": "L2",
    "params": {"nprobe": 10},
}

start_time = time.time()
result = hello_milvus.search(
    vectors_to_search, "embeddings", search_params, limit=3, output_fields=["random"]
)
end_time = time.time()

for hits in result:
    for hit in hits:
        print(f"hit: {hit}, random field: {hit.entity.get('random')}")
print(search_latency_fmt.format(end_time - start_time))

# -----------------------------------------------------------------------------
# query based on scalar filtering(boolean, int, etc.)
print(fmt.format("Start querying with `random > 0.5`"))

start_time = time.time()
result = hello_milvus.query(expr="random > 0.5", output_fields=["random", "embeddings"])
end_time = time.time()

print(f"query result:\n-{result[0]}")
print(search_latency_fmt.format(end_time - start_time))
exit()
# -----------------------------------------------------------------------------
# hybrid search
print(fmt.format("Start hybrid searching with `random > 0.5`"))

start_time = time.time()
result = hello_milvus.search(
    vectors_to_search,
    "embeddings",
    search_params,
    limit=3,
    expr="random > 0.5",
    output_fields=["random"],
)
end_time = time.time()

for hits in result:
    for hit in hits:
        print(f"hit: {hit}, random field: {hit.entity.get('random')}")
print(search_latency_fmt.format(end_time - start_time))

###############################################################################
# 6. delete entities by PK
# You can delete entities by their PK values using boolean expressions.
ids = insert_result.primary_keys

expr = f'pk in ["{ids[0]}" , "{ids[1]}"]'
print(fmt.format(f"Start deleting with expr `{expr}`"))

result = hello_milvus.query(expr=expr, output_fields=["random", "embeddings"])
print(f"query before delete by expr=`{expr}` -> result: \n-{result[0]}\n-{result[1]}\n")

hello_milvus.delete(expr)

result = hello_milvus.query(expr=expr, output_fields=["embeddings"])
print(f"query after delete by expr=`{expr}` -> result: {result}\n")


print(fmt.format("Drop collection `hello_milvus`"))
utility.drop_collection("hello_milvus")
