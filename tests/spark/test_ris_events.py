import json
from uuid import uuid4

import pytest

from analytics.ris import event_summary, peer_activity, provenance, query_events
from lake.ris import EventStore
from schemas.ris import EventChunk
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.ris import RisProcessor, committed_events, exact_projection
from tests.unit.test_ris import owned_rows


@pytest.fixture(scope="module")
def ris_session(tmp_path_factory):
    session = create_session(
        SparkConfig(runtime_dir=tmp_path_factory.mktemp("ris-runtime")), kafka=False
    )
    yield session
    session.stop()


def inputs(session, tmp_path):
    from spark.ris import event_schema

    reference, rows = owned_rows()
    expected = session.createDataFrame(
        [tuple(row.model_dump().values()) for row in rows], event_schema()
    )
    path = str(tmp_path / "expected")
    expected.write.parquet(path)
    return reference, rows, expected, path


def test_exact_replay_conflict_and_occurrence_queries(ris_session, tmp_path):
    reference, rows, expected, path = inputs(ris_session, tmp_path)
    chunk = EventChunk(artifact_sha256=rows[0].artifact_sha256, first_event=0, rows=rows)
    batch = ris_session.createDataFrame([(chunk.model_dump_json(),)], "value STRING")
    store = EventStore(root=tmp_path / "lake")
    query = str(uuid4())
    processor = RisProcessor(
        ris_session, store, query, {rows[0].artifact_sha256: reference}, [path]
    )
    processor.process(batch, 0)
    processor.process(batch, 0)
    processor.process(batch, 1)
    logical = committed_events(ris_session, store, query, expected)
    assert exact_projection(logical, expected, complete=True).count() == 4
    assert sum(commit["logical_records"] for commit in store.inventory(query)) == 8
    assert event_summary(logical)["repeated_fingerprints_beyond_first"] == 2
    assert query_events(logical, start=1, end=2, prefix="192.0.2.0/24").count() == 2
    assert peer_activity(logical).first()["count"] == 4
    assert provenance(logical, rows[0].logical_record_id).first().byte_offset == 0
    changed = chunk.model_dump()
    changed["rows"][0]["attribute_digest"] = "e" * 64
    conflict = ris_session.createDataFrame([(json.dumps(changed),)], "value STRING")
    with pytest.raises(ValueError, match="unexpected/conflicting"):
        processor.process(conflict, 2)
    assert len(store.inventory(query)) == 2


def test_uncommitted_partial_and_corrupt_history(ris_session, tmp_path):
    reference, rows, expected, path = inputs(ris_session, tmp_path)
    chunk = EventChunk(artifact_sha256=rows[0].artifact_sha256, first_event=0, rows=rows)
    batch = ris_session.createDataFrame([(chunk.model_dump_json(),)], "value STRING")
    store = EventStore(root=tmp_path / "lake")
    query = str(uuid4())

    def fault(point):
        return point == "partial_output"

    processor = RisProcessor(
        ris_session, store, query, {rows[0].artifact_sha256: reference}, [path], fault=fault
    )
    with pytest.raises(RuntimeError, match="partial output"):
        processor.process(batch, 0)
    assert committed_events(ris_session, store, query, expected).count() == 0
    processor.fault = lambda point: None
    processor.process(batch, 0)
    assert committed_events(ris_session, store, query, expected).count() == 4
    commit = store.inventory(query)[0]
    expected.limit(1).write.mode("overwrite").parquet(commit["path"])
    with pytest.raises(ValueError, match="corruption"):
        committed_events(ris_session, store, query, expected)
