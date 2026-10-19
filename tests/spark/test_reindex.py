import json

import pytest
from pyspark.sql import functions as F

from lake.config import LakeConfig
from search.config import KINDS, SearchConfig
from search.sink import safe_frame
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.sinks.lake import dataset_frame
from tests.spark.test_lake import batch_outputs


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    spark = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("reindex")),
        kafka=False,
    )
    yield spark
    spark.stop()


@pytest.fixture
def history(session, monkeypatch):
    from lake import client
    from search import reindex
    from spark.streaming.sinks import lake as sink

    frames = {kind: dataset_frame(batch_outputs(session), f"processed/{kind}") for kind in KINDS}
    source_commits = [{"query_id": "owned", "batch_id": 0}]
    reads = []
    writes = []
    monkeypatch.setattr(client, "list_commits", lambda *args: iter(source_commits))

    def read(*args, manifests=None):
        assert manifests == [{"query_id": "owned", "batch_id": 0}]
        reads.append(manifests)
        source_commits.append({"query_id": "later", "batch_id": len(source_commits)})
        return frames[args[2].split("/")[-1]]

    def index(frame, kind, config):
        writes.append(
            (kind, [json.loads(row.document_json) for row in safe_frame(frame, kind).collect()])
        )

    monkeypatch.setattr(sink, "read_committed", read)
    monkeypatch.setattr(reindex, "index_frame", index)
    return frames, reads, writes


def test_rebuild_freezes_input_and_uses_earliest_delivery_for_exact_retries(session, history):
    from search.reindex import rebuild

    frames, reads, writes = history
    first = frames["snapshots"]
    later = first.withColumn("ingest_time", F.col("ingest_time") + F.expr("INTERVAL 1 DAY"))
    frames["snapshots"] = later.unionByName(first).unionByName(first).repartition(2)
    rebuild(session, LakeConfig("owned", "owned"), SearchConfig())
    assert len(reads) == len(KINDS)
    assert all(entry is reads[0] for entry in reads)
    actual = dict(writes)["snapshots"]
    expected = [json.loads(row.document_json) for row in safe_frame(first, "snapshots").collect()]
    assert sorted(actual, key=lambda item: item["snapshot_id"]) == sorted(
        expected, key=lambda item: item["snapshot_id"]
    )
    assert all("normalized_content" not in item for item in actual)


@pytest.mark.parametrize(
    "kind,column", [("snapshots", "normalized_content"), ("alerts", "message")]
)
def test_conflicting_history_fails_before_any_index_writes(session, history, kind, column):
    from search.reindex import rebuild

    frames, _, writes = history
    original = frames[kind]
    frames[kind] = original.unionByName(
        original.withColumn(column, F.concat(F.col(column), F.lit(" changed")))
    )
    with pytest.raises(RuntimeError, match="Conflicting"):
        rebuild(session, LakeConfig("owned", "owned"), SearchConfig())
    assert writes == []
