from pathlib import Path

from redb.init_db import REDB
from redb.document import Document


class Dog(Document):
    name: str
    color: str


def main():
    db_dir = Path("./jsondb/")
    REDB("json", dict(dir_path=db_dir))
    d = Dog(name="Mutley", color="Green")
    print(repr(d))
    d.insert()
    print(Dog.find_many())


if __name__ == "__main__":
    main()
