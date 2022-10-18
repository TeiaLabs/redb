from pathlib import Path

from redb.init_db import REDB
from redb.document import Document


class Dog(Document):
    name: str
    color: str


def main():
    json_path = Path("./db.json")
    REDB("json", dict(file_path=json_path))
    d = Dog(name="Mutley", color="Green")
    d.insert()


if __name__ == "__main__":
    main()
