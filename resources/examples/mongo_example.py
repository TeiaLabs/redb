from __future__ import annotations

import os
import dotenv
from datetime import datetime

from redb import Document, RedB
from redb.mongo_system import MongoConfig

dotenv.load_dotenv()


class Dog(Document):
    name: str
    birthday: datetime
    # friends: list[Dog] = []  # TODO: make this work


def main():
    config = MongoConfig(database_uri=os.environ["MONGODB_URI"])
    RedB.setup(config=config)
    client = RedB.get_client("mongo")
    db = client.get_default_database()
    print([col for col in db.get_collections()])
    d = Dog(
        name="Spike",
        birthday=datetime.today(),
        friends=[Dog(name="Lily", birthday=datetime.today())]
    )
    print(d)
    print(Dog.delete_many(d))
    print(d.insert_one())
    print(Dog.replace_one(filter=d, replacement=d, upsert=True))


if __name__ == "__main__":
    main()
