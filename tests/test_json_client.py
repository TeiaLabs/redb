import pytest
from pathlib import Path

from redb.init_db import REDB
from redb.document import Document


class Embedding(Document):
    kb_name: str
    # metadata: dict[str, str]
    model: str
    text: str
    vector: list[float] = None
    source_url: str

    def get_hash(self):
        return super().get_hash(
            fields=[
                'kb_name',
                'model',
                'text',
                'source_url',
            ],
        )

    @classmethod
    def find_by_id(cls, id):
        return super().find_by_id(id)


@pytest.fixture
def init_test():
    import shutil
    db_dir = Path(".temp/")
    # remove .temp/ with all elements
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir(exist_ok=True)
    return REDB("json", dict(dir_path=db_dir))


def test_insert(init_test):

    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    d.insert()

    current_hash = d.get_hash()

    found_instance = (Embedding.find_by_id(current_hash))
    assert found_instance.get_hash() == current_hash

    # Hash should not take into account the vector
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        source_url="www",
    )

    found_instance = Embedding.find_by_id(d.get_hash())
    assert found_instance.get_hash() == d.get_hash()


def test_insert_many(init_test):
    import pandas as pd

    df_columns = list(Embedding.__fields__.keys())
    df = pd.DataFrame(
        columns=df_columns,
        data=[
            ['KB5', 'ai', 'Some data 5', [1, 3], 'ww5'],
            ['KB6', 'ai', 'Some data 6', [2, 4], 'ww6'],
            ['KB7', 'ai', 'Some data 7', [3, 5], 'ww7'],
        ],
    )
    Embedding.insert_vectors(df)

    df.text = ['a', 'b', 'c']
    dicts = df.to_dict(orient='records')
    Embedding.insert_many(dicts)

    data = Embedding.find_many()
    assert len(data) == 6
    dfa = pd.DataFrame(data=[x.dict() for x in data])
    assert dfa.shape == (6, 5)
    assert set(dfa.columns) == set(df_columns)
