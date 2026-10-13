from uuid import uuid4

import pytest

from lake.dns import DnsStore, read_source_completion
from processing.dns import read_source
from schemas.dns import DnsChunk
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.dns import DnsProcessor, committed_raw
from spark.dns_history import finalize_source, read_table
from tests.unit.test_dns_measurements import owned_source
from workloads.dns_oracle import build


@pytest.fixture(scope="module")
def dns_session(tmp_path_factory):
    session = create_session(
        SparkConfig(
            runtime_dir=tmp_path_factory.mktemp("dns-runtime"),
            master="local[2]",
            driver_memory="2g",
        ),
        kafka=False,
    )
    yield session
    session.stop()


def fixture(session, tmp_path):
    path, source, rows = owned_source(tmp_path)
    output = tmp_path / "oracle"
    build(path, source, output)
    values = tuple(read_source(path, source))
    artifact = source.manifest.artifacts[0].sha256
    store = DnsStore(root=tmp_path / "lake")
    query = str(uuid4())
    processor = DnsProcessor(
        session, store, query, {artifact: source}, [str(output / "raw.parquet")]
    )
    chunks = [
        DnsChunk(artifact_sha256=artifact, rows=values[:3]),
        DnsChunk(artifact_sha256=artifact, rows=values[3:]),
    ]
    batches = [
        session.createDataFrame([(chunk.model_dump_json(),)], "value STRING") for chunk in chunks
    ]
    return source, output, values, store, query, processor, batches


def test_exact_source_group_and_lossless_value_reuse(dns_session, tmp_path):
    source, oracle, values, store, query, processor, batches = fixture(dns_session, tmp_path)
    for index, batch in enumerate([*batches, batches[0]]):
        processor.process(batch, index)
    with committed_raw(dns_session, store, query, processor.expected, complete=True) as raw:
        assert raw.count() == 6
        assert raw.where("address_value='192.0.2.1'").count() == 2
    descriptor = finalize_source(dns_session, store, query, source, oracle)
    artifact = source.manifest.artifacts[0].sha256
    assert descriptor["source_rows"] == 6 and descriptor["address_groups"] == 2
    assert read_source_completion(store, query, artifact)["entity_coverage"] == "unknown"
    full = read_table(dns_session, store, query, artifact, "groups")
    reused = read_table(dns_session, store, query, artifact, "reuse")
    data = read_table(dns_session, store, query, artifact, "values")
    assert data.count() == 1
    assert reused.join(data, "value_set_id").select(*full.columns).exceptAll(full).count() == 0
    assert read_table(dns_session, store, query, artifact, "contributors").count() == 2
    inventory = store.inventory(query)
    processor.process(batches[0], 99)
    assert store.inventory(query) == inventory
    assert read_source_completion(store, query, artifact)["source_rows"] == 6


def test_independent_comparison_ledger(dns_session, tmp_path):
    import hashlib
    from datetime import date

    import pyarrow as arrow
    import pyarrow.parquet as parquet

    from analytics.dns import compare_groups
    from spark.dns import verify_projection
    from workloads.dns_oracle import comparison_ledger

    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    path, source, rows = owned_source(first)
    build(path, source, first / "oracle")
    rows[0]["ip4_address"] = "192.0.2.2"
    other_path = second / "owned.parquet"
    parquet.write_table(arrow.Table.from_pylist(rows), other_path, row_group_size=2)
    artifact = source.manifest.artifacts[0].model_copy(
        update={
            "sha256": hashlib.sha256(other_path.read_bytes()).hexdigest(),
            "bytes": other_path.stat().st_size,
        }
    )
    other = source.model_copy(
        update={
            "partition_date": date(2024, 1, 2),
            "manifest": source.manifest.model_copy(update={"artifacts": (artifact,)}),
        }
    )
    build(other_path, other, second / "oracle")
    ledger = tmp_path / "comparison.parquet"
    assert comparison_ledger(first / "oracle", second / "oracle", ledger)["rows"] == 2
    previous = dns_session.read.parquet(str(first / "oracle/groups.parquet"))
    current = dns_session.read.parquet(str(second / "oracle/groups.parquet"))
    actual, summary = compare_groups(previous, current)
    assert summary["changed_groups"] == 1
    expected = dns_session.read.parquet(str(ledger)).select(*actual.columns)
    with verify_projection(actual, expected, complete=True) as exact:
        assert exact.count() == 2


@pytest.mark.parametrize(
    "failure", ["before_output", "partial_output", "before_commit", "after_commit"]
)
def test_source_callback_recovery(dns_session, tmp_path, failure):
    source, oracle, values, store, query, processor, batches = fixture(dns_session, tmp_path)

    def fault(point):
        if point == failure:
            if failure == "partial_output":
                return True
            raise RuntimeError("owned DNS callback interruption")
        return False

    processor.fault = fault
    with pytest.raises(RuntimeError):
        processor.process(batches[0], 0)
    processor.fault = lambda point: False
    processor.process(batches[0], 0)
    processor.process(batches[0], 0)
    processor.process(batches[1], 1)
    assert len(store.inventory(query)) == 2
    assert finalize_source(dns_session, store, query, source, oracle)["source_rows"] == 6


def test_incomplete_source_and_corrupt_group_fail_closed(dns_session, tmp_path):
    source, oracle, values, store, query, processor, batches = fixture(dns_session, tmp_path)
    processor.process(batches[0], 0)
    with pytest.raises(ValueError):
        finalize_source(dns_session, store, query, source, oracle)
    artifact = source.manifest.artifacts[0].sha256
    assert store.get(f"completion/{query}/{artifact}") is None
    processor.process(batches[1], 1)
    finalize_source(dns_session, store, query, source, oracle)
    commit = store.get(f"tables/{query}/values/{artifact}")
    table = read_table(dns_session, store, query, artifact, "values")
    table.limit(0).write.mode("overwrite").parquet(commit["path"])
    with pytest.raises(ValueError):
        read_table(dns_session, store, query, artifact, "values")
