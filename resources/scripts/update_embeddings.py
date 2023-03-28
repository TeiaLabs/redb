import argparse
import logging

from melting_face.encoders import LocalSettings
from redb.core import RedB, MongoConfig
from redb.teia_schema.knowledge_base import KnowledgeBaseManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Knowledge base manager CLI interface (MongoDB only for now).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--db_uri",
        type=str,
        required=True,
        help="MongoDB URI to connect to.",
    )
    parser.add_argument(
        "--db_name",
        type=str,
        required=True,
        help="Name of the database to use.",
    )
    parser.add_argument(
        "--model_type",
        type=str,
        required=True,
        help="Type of the model to use.",
    )
    parser.add_argument(
        "--model_name",
        type=str,
        required=True,
        help="Name of the model to use.",
    )
    parser.add_argument(
        "--kb_name",
        type=str,
        default=None,
        help="Names of sub-kbs to compute embeddings for.",
    )
    parser.add_argument(
        "--overwrite_embeddings",
        action="store_true",
        help="Whether to overwrite embeddings for all instances.",
    )
    parser.add_argument(
        "--use_gpu",
        action="store_true",
        help="Whether to use GPU to compute embeddings.",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1,
        help="Batch size to compute embeddings.",
    )
    args = parser.parse_args()
    return args


def _run_cli(
    database_uri: str,
    database_name: str,
    model_type: str,
    model_name: str,
    use_gpu: bool,
    overwrite: bool,
    batch_size: int,
    kb_name: str,
):
    logging.basicConfig(level=logging.DEBUG)
    redb_config = MongoConfig(
        database_uri,
        default_database=database_name,
    )
    RedB.setup(redb_config)
    model_config = LocalSettings(
        model_type=model_type,
        model_kwargs=dict(
            model_name=model_name,
            device="cuda" if use_gpu else "cpu",
        ),
    )
    kb_manager = KnowledgeBaseManager(
        model_config=model_config,
        search_settings=None,
        database_name=database_name,
        preload_local_kb=False,
    )

    updated_ids = kb_manager.update_instance_embeddings(
        kb_name=kb_name,
        overwrite=overwrite,
        update_local_kb=False,
        batch_size=batch_size,
    )


if __name__ == "__main__":
    args = parse_args()
    _run_cli(
        database_uri=args.db_uri,
        database_name=args.db_name,
        model_type=args.model_type,
        model_name=args.model_name,
        use_gpu=args.use_gpu,
        overwrite=args.overwrite_embeddings,
        batch_size=args.batch_size,
        kb_name=args.kb_name,
    )
