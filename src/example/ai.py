from pathlib import Path

from redb.init_db import REDB
from redb.document import Document


class Embedding(Document):
    kb_name: str
    # metadata: dict[str, str]
    model: str
    text: str
    vector: list[float]
    source_url: str


def main():
    db_dir = Path("./jsondb/")
    REDB("json", dict(dir_path=db_dir))
    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )
    # print(repr(d))
    d.insert()
    print(Embedding.find_many())


if __name__ == "__main__":
    main()
