import argparse
from dataclasses import replace

from lake.config import LakeConfig
from search.client import initialize_indices
from search.config import KINDS, SearchConfig
from search.sink import index_frame
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


def rebuild(session, lake, search: SearchConfig, query_id: str | None = None) -> None:
    from spark.streaming.sinks.lake import read_committed

    for kind in KINDS:
        index_frame(read_committed(session, lake, f"processed/{kind}", query_id), kind, search)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild safe search projections from committed Parquet"
    )
    parser.add_argument("--query-id", help="Limit to one checkpointed lake query")
    args = parser.parse_args()
    search = SearchConfig.from_env()
    initialize_indices(search)
    config = replace(SparkConfig.from_env(), lake=LakeConfig.from_env())
    session = create_session(config, kafka=False)
    try:
        rebuild(session, config.lake, search, args.query_id)
    finally:
        session.stop()


if __name__ == "__main__":
    main()
