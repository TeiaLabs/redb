from __future__ import annotations

from pathlib import Path

from src.redb import JSONCollection, RedB


class Embedding(JSONCollection):
    __database_name__ = "batata"

    kb_name: str
    model: str
    text: str
    vector: list[float]
    source_url: str


def main():
    db_dir = Path("./jsondb/")
    RedB.setup("json", dict(dir_path=db_dir))

    d = Embedding(
        kb_name="KB",
        model="ai",
        text="Some data.",
        vector=[1, 2],
        source_url="www",
    )

    d.insert_one()
    print(Embedding.find_one())


if __name__ == "__main__":
    main()
