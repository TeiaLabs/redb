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
    # Dog(name=Mutley, color=Green)
    d.insert()
    print(Dog.find_many())
    # []
    # find_many is returning an empty list
    # because it's looking for the JSONDocument class name
    # instead of the Dog class name.


if __name__ == "__main__":
    main()
