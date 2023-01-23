from typing import Optional

import pandas as pd
from pydantic import BaseModel

from redb import Document
from redb.interface.fields import ClassField, CompoundIndex, Index


class Embedding(BaseModel):
    model_name: str
    model_type: str
    vector: list[float]


class Instance(Document):
    content_embedding: Optional[list[Embedding]]
    content: str
    data_type: Optional[str] = "text"
    kb_name: Optional[str]
    query_embedding: Optional[list[Embedding]] = []
    query: Optional[str]
    url: Optional[str]  # TODO: this should be a set/list

    @classmethod
    def get_hashable_fields(cls) -> list[ClassField]:
        fields = [
            cls.kb_name,
            cls.content,
        ]
        return fields

    @classmethod
    def get_indexes(cls) -> list[Index | CompoundIndex]:
        return [Index(field=cls.id)]

    @classmethod
    def content_embedding_name(cls):
        return "content_embedding"

    @classmethod
    def query_embedding_name(cls):
        return "query_embedding"

    @staticmethod
    def explode_embeddings(df, embedding_name):
        new_df = df.explode(embedding_name)
        new_df[["model_type", "model_name", "vector"]] = new_df[embedding_name].apply(
            pd.Series
        )
        new_df.drop(columns=[embedding_name], inplace=True)
        return new_df

    @classmethod
    def get_kb_instances(cls, kb_name: str) -> list[object]:
        return cls.find({"kb_name": kb_name})

    @classmethod
    def get_kb_names(cls) -> set:
        return set(cls.distinct(key="kb_name"))

    @classmethod
    def get_model_instances(cls, model_type, model_name):
        return cls.find({
            "content_embedding.model_type": model_type,
            "content_embedding.model_name": model_name,
        })

    @classmethod
    def instances_to_dataframe(
        cls, instances: list[object], explode_vectors=True
    ) -> pd.DataFrame:
        if len(instances) == 0:
            return pd.DataFrame(data={})

        instances = [inst.dict() for inst in instances]
        df = pd.DataFrame(instances)
        if explode_vectors:
            df = cls.explode_embeddings(df, cls.content_embedding_name())
            if df[cls.query_embedding_name()].any():
                df = cls.explode_embeddings(df, cls.query_embedding_name())
        return df

    @classmethod
    def from_row(cls, dataframe_row: pd.DataFrame):

        content_embedding = None
        if (
            "content_embedding.vector" in dataframe_row
            and dataframe_row["content_embedding.vector"] is not None
        ):
            content_embedding = [
                Embedding(
                    model_name=dataframe_row["content_embedding.model_name"],
                    model_type=dataframe_row["content_embedding.model_type"],
                    vector=dataframe_row["content_embedding.vector"],
                )
            ]

        inst = cls(content_embedding=content_embedding, **dataframe_row.to_dict())
        return inst

    @classmethod
    def from_df(cls, dataframe: pd.DataFrame) -> list:
        return [cls.from_row(x[1]) for x in dataframe.iterrows()]

    @classmethod
    def insert_dataframe(cls, dataframe: pd.DataFrame) -> list:
        instances = [
            cls.from_row(row[1]) for row in dataframe.iterrows()
        ]

        return cls.insert_many(instances)
