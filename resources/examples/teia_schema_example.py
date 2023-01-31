import random
from datetime import datetime

from redb.core import RedB
from redb.interface.configs import JSONConfig
from redb.teia_schema import Embedding, File, Instance, Mirror


def main():
    random.seed(0)

    config = JSONConfig(
        client_folder_path="resources",
        default_database_folder_path="redb",
    )
    RedB.setup(config, backend="json")

    # inserting a file
    f = File(
        scraped_at=datetime.now(),
        last_modified_at=datetime.now(),
        hash=random.getrandbits(128),
        organization_id="teia",
        size_bytes=128,
        url_mirrors=[
            Mirror(source="teia", url="https://teialabs.com/file.txt")
        ],
        url_original="www.github.com/org/repo/file1.txt",
    )
    res = File.update_one(f, f, upsert=True)

    # inserting file instances
    d = Instance(
        content_embedding=[
            Embedding(
                model_name="openai",
                model_type="davinci",
                vector=[1.0, 2.0, 3.0, 4.0],
            ),
        ],
        content="Some data.",
        data_type="text",
        file_id=f.id,
        kb_name="KB",
        query="Some query.",
        query_embedding=[
            Embedding(
                model_name="openai",
                model_type="davinci",
                vector=[4.0, 3.0, 2.0, 1.0],
            )
        ],
        url="www.github.com/org/repo/file1.txt",
    )
    res = Instance.update_one(d, d, upsert=True)

    d = Instance(
        content_embedding=[],
        content="Other data.",
        data_type="text",
        file_id=f.id,
        kb_name="KB",
        query="Another query.",
        query_embedding=[],
        url="www.github.com/org/repo/file1.txt",
    )
    res = Instance.update_one(d, d, upsert=True)

    # testing helper methods
    orgs = File.get_organization_ids()
    print(orgs)

    instances = File.get_file_instances(f.id)
    print(instances)


if __name__ == "__main__":
    main()
