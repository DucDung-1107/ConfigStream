import argparse
from dataclasses import replace

from lake.config import LakeConfig
from search.client import initialize_indices
from search.config import KINDS, SearchConfig
from search.sink import index_frame
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


def rebuild(session, lake, search: SearchConfig, query_id: str | None = None) -> None:
    from pyspark.sql import functions as F

    from lake.client import list_commits
    from lake.reconciliation import reconcile_history
    from spark.streaming.sinks.lake import read_committed

    manifests = list(list_commits(lake, query_id))
    inputs = {}
    identifiers = {"snapshots": "snapshot_id", "diffs": "diff_id", "alerts": "alert_id"}
    for kind in KINDS:
        frame = read_committed(session, lake, f"processed/{kind}", query_id, manifests=manifests)
        if kind in identifiers:
            reconciled = reconcile_history(frame, identifiers[kind])
            if kind == "snapshots":
                delivery = frame.groupBy("snapshot_id").agg(
                    F.min("ingest_time").alias("ingest_time")
                )
                reconciled = reconciled.join(delivery, "snapshot_id")
            frame = reconciled
        inputs[kind] = frame
    for kind, frame in inputs.items():
        index_frame(frame, kind, search)


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
