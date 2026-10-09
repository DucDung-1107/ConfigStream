import argparse
import json
from dataclasses import replace

from lake.config import LakeConfig
from spark.common.config import SparkConfig
from spark.common.lake_schemas import DATASET_SCHEMAS
from spark.common.runtime import create_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Count committed historical Parquet rows")
    parser.add_argument("--query-id", help="Limit inspection to one checkpointed query")
    args = parser.parse_args()
    config = replace(SparkConfig.from_env(), lake=LakeConfig.from_env())
    session = create_session(config, kafka=False)
    from spark.streaming.sinks.lake import read_committed

    try:
        counts = {
            dataset: read_committed(session, config.lake, dataset, args.query_id).count()
            for dataset in DATASET_SCHEMAS
        }
        print(json.dumps(counts, sort_keys=True))
    finally:
        session.stop()


if __name__ == "__main__":
    main()
