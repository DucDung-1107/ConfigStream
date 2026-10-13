import pytest

from schemas.ris import EventRow
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.ris import (
    bounded_projection,
    event_schema,
    exact_projection,
    projection_digest,
    reconciliation_input,
)
from tests.unit.test_ris import owned_rows


@pytest.fixture(scope="module")
def final_session(tmp_path_factory):
    session = create_session(
        SparkConfig(
            runtime_dir=tmp_path_factory.mktemp("ris-finalization"),
            driver_memory="2g",
            master="local[2]",
        ),
        kafka=False,
    )
    yield session
    session.stop()


def frame(session, rows):
    return session.createDataFrame(
        [tuple(row.model_dump().values()) for row in rows], event_schema()
    )


def test_exact_equivalence_and_digest(final_session):
    _, rows = owned_rows()
    expected = frame(final_session, rows)
    actual = frame(final_session, [*rows, *reversed(rows)]).repartition(3)
    old = exact_projection(actual, expected, complete=True)
    metrics = {}
    with bounded_projection(actual, expected, partitions=4, metrics=metrics) as new:
        assert new.count() == 4
        assert projection_digest(new) == projection_digest(old)
        assert (
            projection_digest(new)[1]
            == "4a4c5f1bc228e15f37f852d0e70df5fb87e6c80f35504194aa87d51c5ee5479a"
        )
        assert not new.exceptAll(expected).count()
    assert metrics["counts"] == {"accepted": 4}
    plan = reconciliation_input(actual, expected, 4)._jdf.queryExecution().executedPlan().toString()
    assert "Sort" in plan and "hashpartitioning(logical_record_id" in plan
    assert "HashAggregate" not in plan


@pytest.mark.parametrize("case", ["missing", "extra", "conflict", "wrong_artifact"])
def test_old_new_negative_agreement(final_session, case):
    _, rows = owned_rows()
    expected = frame(final_session, rows)
    values = rows[:-1] if case == "missing" else [*rows]
    if case != "missing":
        field, value = {
            "extra": ("logical_record_id", "f" * 64),
            "conflict": ("attribute_digest", "c" * 64),
            "wrong_artifact": ("artifact_sha256", "b" * 64),
        }[case]
        values.append(rows[0].model_copy(update={field: value}))
    actual = frame(final_session, values).repartition(3, "artifact_sha256")
    with pytest.raises(ValueError):
        exact_projection(actual, expected, complete=True)
    with pytest.raises(ValueError):
        with bounded_projection(actual, expected, partitions=4):
            pytest.fail("invalid projection accepted")


def test_skewed_equal_fingerprints_and_distant_retry(final_session):
    from pyspark.sql import functions as functions

    _, rows = owned_rows()
    seed = frame(final_session, rows[:1])
    expected = (
        final_session.range(10000)
        .crossJoin(seed)
        .withColumn("logical_record_id", functions.sha2(functions.col("id").cast("string"), 256))
        .select(*EventRow.model_fields)
    )
    actual = expected.unionByName(expected.limit(1)).repartition(2)
    with bounded_projection(actual, expected, partitions=8) as result:
        assert result.count() == 10000
        assert result.select("content_fingerprint").distinct().count() == 1


def test_partition_limits(final_session):
    _, rows = owned_rows()
    expected = frame(final_session, rows)
    for invalid in (True, 0, 1, 257):
        with pytest.raises(ValueError, match="partition bound"):
            reconciliation_input(expected, expected, invalid)


def retained_fixture(session, tmp_path):
    import json
    from hashlib import sha256
    from uuid import uuid4

    from lake.ris import EventStore, metadata_digest
    from tests.unit.test_ris_closure import accounting_fixture

    source, summary, exclusions = accounting_fixture()
    _, rows = owned_rows()
    expected = frame(session, rows)
    values = [row.model_dump() for row in rows]

    def independent_digest(columns):
        return sha256(
            b"".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                for row in sorted(values, key=lambda row: tuple(row[column] for column in columns))
            )
        ).hexdigest()

    summary["source_order_projection_sha256"] = independent_digest(
        ["byte_offset", "expansion_ordinal"]
    )
    store, query = EventStore(root=tmp_path / "lake"), str(uuid4())
    store.put(f"sources/{source.manifest.manifest_id}", source.model_dump(mode="json"))
    path = store.path(f"{query}/00000000000000000000")
    expected.write.parquet(path)
    commit = dict(
        format_version="ris-1.1",
        query_id=query,
        batch_id=0,
        path=path,
        logical_records=4,
        projection_sha256=independent_digest(["logical_record_id"]),
        provenance_ids=[source.manifest.manifest_id],
        source_artifact_sha256s=[source.manifest.artifacts[0].sha256],
    )
    store.put(f"batches/{query}/00000000000000000000", commit)
    artifact = source.manifest.artifacts[0].sha256
    arguments = dict(
        session=session,
        store=store,
        query_id=query,
        expected=expected,
        references={artifact: source},
        summaries={artifact: summary},
        exclusions={artifact: exclusions},
        inventory_sha256=metadata_digest(store.inventory(query)),
        partitions=4,
    )
    return arguments, commit


@pytest.mark.parametrize("failure", ["before_accounting", "after_accounting", "before_window"])
def test_finalization_interrupt_retry_and_immutable_history(final_session, tmp_path, failure):
    from benchmarks.ris_service import finalize_history
    from lake.ris import read_window

    arguments, commit = retained_fixture(final_session, tmp_path)
    store, query = arguments["store"], arguments["query_id"]

    def fault(point):
        if point == failure:
            raise RuntimeError("owned recovery interruption")

    with pytest.raises(RuntimeError, match="owned recovery"):
        finalize_history(**arguments, fault=fault)
    assert store.get(f"windows/{query}") is None
    result = finalize_history(**arguments)
    assert result["logical_occurrences"] == 4
    assert (
        result["projection_sha256"]
        == "4a4c5f1bc228e15f37f852d0e70df5fb87e6c80f35504194aa87d51c5ee5479a"
    )
    assert finalize_history(**arguments)["window_descriptor"] == read_window(store, query)
    assert store.inventory(query) == [commit]


@pytest.mark.parametrize(
    "damage",
    [
        "missing_batch",
        "missing_file",
        "corrupt_projection",
        "missing_source",
        "corrupt_source",
        "wrong_digest",
        "wrong_policy",
    ],
)
def test_retained_history_integrity_rejection(final_session, tmp_path, damage):
    import json
    from pathlib import Path

    from py4j.protocol import Py4JJavaError
    from pyspark.errors import AnalysisException

    from benchmarks.ris_service import finalize_history

    arguments, commit = retained_fixture(final_session, tmp_path)
    store, query = arguments["store"], arguments["query_id"]
    source = next(iter(arguments["references"].values()))
    source_path = store.root / store.key(f"sources/{source.manifest.manifest_id}")
    if damage == "missing_batch":
        (store.root / store.key(f"batches/{query}/00000000000000000000")).unlink()
    elif damage == "missing_file":
        next(Path(commit["path"]).glob("*.parquet")).unlink()
    elif damage == "corrupt_projection":
        arguments["expected"].limit(1).write.mode("overwrite").parquet(commit["path"])
    elif damage == "missing_source":
        source_path.unlink()
    elif damage == "corrupt_source":
        source_path.write_text(json.dumps({}))
    elif damage == "wrong_digest":
        arguments["inventory_sha256"] = "f" * 64
    else:
        next(iter(arguments["summaries"].values()))["parser_version"] = "different"
    with pytest.raises((ValueError, AnalysisException, Py4JJavaError)):
        finalize_history(**arguments)
    assert store.get(f"windows/{query}") is None


def test_recovery_duplicate_batches_and_sidecar_corruption(final_session, tmp_path):
    from benchmarks.ris_service import finalize_history
    from lake.ris import metadata_digest

    arguments, commit = retained_fixture(final_session, tmp_path)
    store, query = arguments["store"], arguments["query_id"]
    later = commit | {"batch_id": 900, "path": store.path(f"{query}/00000000000000000900")}
    arguments["expected"].write.parquet(later["path"])
    store.put(f"batches/{query}/00000000000000000900", later)
    arguments["inventory_sha256"] = metadata_digest(
        sorted(store.inventory(query), key=lambda row: row["batch_id"])
    )
    result = finalize_history(**arguments)
    assert result["logical_occurrences"] == 4
    assert sum(row["logical_records"] for row in store.inventory(query)) == 8
    artifact = next(iter(arguments["references"]))
    sidecar = store.root / store.key(f"exclusions/{artifact}")
    sidecar.unlink()
    with pytest.raises(ValueError, match="exclusion"):
        finalize_history(**arguments)
    sidecar.write_text('[{"corrupt":true}]')
    with pytest.raises(ValueError):
        finalize_history(**arguments)


def test_cross_archive_identity_conflict_and_multiplicity(final_session):
    from schemas.ris import RisReference
    from schemas.temporal.provenance import SourceManifest

    source, rows = owned_rows()
    manifest = source.manifest.model_dump()
    manifest["artifacts"] = [manifest["artifacts"][0] | {"sha256": "b" * 64}]
    second = RisReference(manifest=SourceManifest.model_validate(manifest))
    other = [
        row.model_copy(
            update={
                "artifact_sha256": "b" * 64,
                "provenance_manifest_id": second.manifest.manifest_id,
                "logical_record_id": second.occurrence_id(row.byte_offset, row.expansion_ordinal),
            }
        )
        for row in rows
    ]
    expected = frame(final_session, [*rows, *other])
    actual = frame(final_session, [*rows, *other, *rows]).repartition(2, "artifact_sha256")
    with bounded_projection(actual, expected, partitions=4) as result:
        assert result.count() == 8
        assert result.select("content_fingerprint").distinct().count() == 2
    conflict = other[0].model_copy(update={"logical_record_id": rows[0].logical_record_id})
    wrong = actual.unionByName(frame(final_session, [conflict])).repartition(2, "artifact_sha256")
    with pytest.raises(ValueError, match="conflict"):
        with bounded_projection(wrong, expected, partitions=4):
            pytest.fail("cross-archive identity conflict accepted")
