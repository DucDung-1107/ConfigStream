"""RPKI distributed transformations and real local checkpoint fault tests."""

import json

import pytest

from lake.rpki import LocalStore
from schemas.rpki import DayReference
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.rpki import (
    committed_rows,
    comparable,
    decode_chunks,
    finalize_day,
    publish_boundary,
    publish_raw,
    read_state,
    reconcile_rows,
    reconstruct,
    transition_ids,
)
from workloads.rpki_oracle import run_oracle
from workloads.rpki_source import source_chunks


@pytest.fixture(scope="module")
def rpki_session(tmp_path_factory):
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("rpki-runtime")),
        kafka=False,
    )
    yield session
    session.stop()


def owned_inputs(session, fixture):
    cache, selection, output, _, references = fixture
    summary = run_oracle(selection, cache, output)
    chunks = []
    for reference, artifact in zip(references.values(), selection["artifacts"], strict=True):
        chunks.extend(
            chunk.model_dump_json()
            for chunk in source_chunks(cache / artifact["file"], reference, chunk_size=2)
        )
    frame = decode_chunks(
        session.createDataFrame([(value,) for value in reversed(chunks + chunks)], "value STRING"),
        references,
    )
    return frame, references, output, summary


@pytest.mark.parametrize("reuse", [False, True])
def test_callback_reuse_old_retry_conflict_completion_and_restart(
    rpki_session, rpki_fixture, tmp_path, monkeypatch, reuse
):
    from spark.rpki_stream import RpkiProcessor

    session = rpki_session
    cache, selection, oracle, _, references = rpki_fixture
    summary = run_oracle(selection, cache, oracle)
    first, second = references.values()
    chunks = list(source_chunks(cache / selection["artifacts"][0]["file"], first, chunk_size=2))
    store = LocalStore(tmp_path / "lake")
    query = "00000000-0000-4000-8000-000000000071"

    def processor():
        return RpkiProcessor(
            session,
            store,
            query,
            references,
            oracle,
            summary["boundaries"],
            reuse_verified_rows=reuse,
        )

    def batch(chunk):
        return session.createDataFrame([(chunk.model_dump_json(),)], "value STRING")

    processor()(batch(chunks[1]), 0)
    assert store.get("state/owned.tal/2026-09-01") is None
    processor()(batch(chunks[1]), 1)
    assert committed_rows(session, store, query).count() == 2
    assert store.get("state/owned.tal/2026-09-01") is None
    put = store.put

    def fail_after_raw(key, value):
        put(key, value)
        if value.get("dataset") == "source_rows":
            raise RuntimeError("owned crash after raw commit")

    monkeypatch.setattr(store, "put", fail_after_raw)
    with pytest.raises(RuntimeError, match="after raw commit"):
        processor()(batch(chunks[0]), 2)
    assert store.get("state/owned.tal/2026-09-01") is None
    before = {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}
    monkeypatch.setattr(store, "put", put)
    processor()(batch(chunks[0]), 2)
    assert all(path.read_bytes() == content for path, content in before.items())
    assert committed_rows(session, store, query).count() == 4
    assert read_state(session, store, first).count() == 3
    processor()(batch(chunks[1]), 3)
    assert committed_rows(session, store, query).count() == 4
    from schemas.rpki import chunk_identity

    bad_row = chunks[0].rows[0].model_copy(update={"member": chunks[1].rows[1].member})
    bad_rows = (bad_row, chunks[0].rows[1])
    bad = chunks[0].model_copy(
        update={
            "rows": bad_rows,
            "chunk_id": chunk_identity(first, chunks[0].first_row, bad_rows),
        }
    )
    with pytest.raises(ValueError, match="coordinate oracle"):
        processor()(batch(bad), 4)
    assert store.get(f"raw/{query}/00000000000000000004") is None
    other = list(source_chunks(cache / selection["artifacts"][1]["file"], second, chunk_size=2))
    processor()(
        session.createDataFrame(
            [(chunk.model_dump_json(),) for chunk in reversed(other)], "value STRING"
        ),
        5,
    )
    assert committed_rows(session, store, query).count() == 7
    assert reconstruct(session, store, [first, second]).count() == 3
    assert store.get(f"boundary/{first.observation_id}/{second.observation_id}")["rows"] == 2


def test_reuse_completion_rechecks_written_physical_ledger(
    rpki_session, rpki_fixture, tmp_path, monkeypatch
):
    from pyspark.sql import functions as functions

    import spark.rpki_stream as stream

    session = rpki_session
    cache, selection, oracle, _, references = rpki_fixture
    run_oracle(selection, cache, oracle)
    first = next(iter(references.values()))
    chunks = list(source_chunks(cache / selection["artifacts"][0]["file"], first, chunk_size=2))
    store = LocalStore(tmp_path / "lake")
    original = stream.publish_raw

    def corrupt_after_publication(frame, *args):
        commit = original(frame, *args)
        frame.withColumn("provenance_id", functions.lit("f" * 64)).write.mode("overwrite").parquet(
            commit["path"]
        )
        return commit

    monkeypatch.setattr(stream, "publish_raw", corrupt_after_publication)
    processor = stream.RpkiProcessor(
        session, store, "00000000-0000-4000-8000-000000000072", references, oracle, []
    )
    with pytest.raises(ValueError, match="ledger checksum"):
        processor(
            session.createDataFrame(
                [(chunk.model_dump_json(),) for chunk in chunks], "value STRING"
            ),
            0,
        )
    assert store.get("state/owned.tal/2026-09-01") is None


def test_complete_states_exact_transition_reconstruction_and_negative_barriers(
    rpki_session, rpki_fixture, tmp_path, monkeypatch
):
    session = rpki_session
    frame, references, output, summary = owned_inputs(session, rpki_fixture)
    frame = frame.persist()
    store = LocalStore(tmp_path / "lake")
    first, second = references.values()
    expected = session.read.parquet(str(output / "members/2026-09-01.parquet"))
    raw_map = session.read.parquet(str(output / "source_maps/2026-09-01.parquet"))
    try:
        assert (
            finalize_day(
                frame.filter("row_index < 4"), first, expected, store, expected_rows=raw_map
            )
            is None
        )
        with pytest.raises(ValueError, match="incomplete"):
            read_state(session, store, first)
        with pytest.raises(ValueError, match="source-coordinate"):
            finalize_day(frame, first, expected, store)
        bad = first.model_dump(mode="json")
        bad["observation"]["payload"]["set_digest"] = "f" * 64
        with pytest.raises(ValueError, match="digest"):
            finalize_day(
                frame, DayReference.model_validate(bad), expected, store, expected_rows=raw_map
            )
        original_put = store.put

        def fail_before_completion(key, value):
            raise RuntimeError("owned failure before day completion")

        monkeypatch.setattr(store, "put", fail_before_completion)
        with pytest.raises(RuntimeError, match="completion"):
            finalize_day(frame, first, expected, store, expected_rows=raw_map)
        assert list((tmp_path / "lake").rglob("*.parquet"))
        with pytest.raises(ValueError, match="incomplete"):
            read_state(session, store, first)
        monkeypatch.setattr(store, "put", original_put)
        finalize_day(frame, first, expected, store, expected_rows=raw_map)
        assert read_state(session, store, first).count() == 3
        before = {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}
        finalize_day(frame, first, expected, store, expected_rows=raw_map)
        assert before == {
            path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")
        }
        finalized = finalize_day(
            frame,
            second,
            session.read.parquet(str(output / "members/2026-09-02.parquet")),
            store,
            expected_rows=session.read.parquet(str(output / "source_maps/2026-09-02.parquet")),
        )
        assert finalized["source_rows"] == 3
        with pytest.raises(ValueError, match="Missing"):
            reconstruct(session, store, [first, second])
        commit = publish_boundary(session, store, first, second, summary["boundaries"][0])
        assert commit["additions"] == commit["removals"] == 1 and commit["rows"] == 2
        restored = reconstruct(session, store, [first, second])
        assert {tuple(row) for row in restored.select("asn", "prefix", "max_length").collect()} == {
            (64500, "192.0.2.0/24", 24),
            (64501, "198.51.100.0/24", 24),
            (64503, "203.0.113.0/24", 24),
        }
        transitions = session.read.parquet(commit["path"])
        assert {tuple(row) for row in transitions.select("action", "asn", "prefix").collect()} == {
            ("add", 64503, "203.0.113.0/24"),
            ("remove", 64502, "2001:db8::/32"),
        }
        assert publish_boundary(session, store, first, second, summary["boundaries"][0]) == commit
        from analytics.rpki import asn_activity, authorization_at, distinct_memberships
        from schemas.temporal.records import Authorization

        assert authorization_at(
            session,
            store,
            first,
            Authorization(scope="owned.tal", asn=64500, prefix="192.0.2.0/24", max_length=24),
        )
        assert distinct_memberships(session, store, [first, second]) == 4
        assert {tuple(row) for row in asn_activity(session, store, first, second).collect()} == {
            (64503, "add", 1),
            (64502, "remove", 1),
        }
        with pytest.raises(ValueError, match="oracle"):
            publish_boundary(
                session, store, first, second, summary["boundaries"][0] | {"additions": 2}
            )
        from schemas.temporal.records import Evidence, evidence_identity

        changed = second.observation.model_copy(
            update={
                "payload": second.observation.payload.model_copy(
                    update={"validation_regime": "different"}
                )
            }
        )
        changed = Evidence(
            envelope=changed.envelope.model_copy(
                update={"logical_record_id": evidence_identity(changed, second.manifest)}
            ),
            payload=changed.payload,
        )
        mismatched = DayReference(manifest=second.manifest, observation=changed)
        with pytest.raises(ValueError, match="comparable"):
            comparable(first, mismatched)
    finally:
        frame.unpersist()


def test_raw_replay_conflict_integrity_and_unpublished_output(
    rpki_session, rpki_fixture, tmp_path, monkeypatch
):
    from pyspark.sql import functions as functions

    session = rpki_session
    frame, _, _, _ = owned_inputs(session, rpki_fixture)
    store = LocalStore(tmp_path / "lake")
    query = "00000000-0000-4000-8000-000000000017"
    put = store.put

    def fail(key, value):
        raise RuntimeError("owned failure before raw manifest")

    monkeypatch.setattr(store, "put", fail)
    with pytest.raises(RuntimeError, match="manifest"):
        publish_raw(frame, store, query, 0)
    assert committed_rows(session, store, query).count() == 0
    monkeypatch.setattr(store, "put", put)
    commit = publish_raw(frame, store, query, 0)
    assert commit["rows"] == 7
    assert publish_raw(frame, store, query, 0) == commit
    assert committed_rows(session, store, query).count() == 7
    with pytest.raises(ValueError, match="Conflicting source-row retry"):
        reconcile_rows(
            frame.unionByName(frame.withColumn("provenance_id", functions.lit("f" * 64)))
        )
    with pytest.raises(ValueError, match="replay conflict"):
        publish_raw(frame.filter("row_index < 4"), store, query, 0)
    with pytest.raises(ValueError, match="checksum"):
        publish_raw(frame.withColumn("prefix", functions.lit("203.0.113.0/24")), store, query, 1)
    assert not store.get(f"raw/{query}/00000000000000000001")


def test_local_spark_checkpoint_interruption_after_commit_and_retry(
    rpki_session, rpki_fixture, tmp_path
):
    from pyspark.errors import StreamingQueryException

    session = rpki_session
    cache, selection, _, _, references = rpki_fixture
    reference = next(iter(references.values()))
    source = tmp_path / "input"
    source.mkdir()
    (source / "owned.json").write_text(
        "\n".join(
            chunk.model_dump_json()
            for chunk in source_chunks(
                cache / selection["artifacts"][0]["file"], reference, chunk_size=2
            )
        )
    )
    store = LocalStore(tmp_path / "lake")
    identity = "00000000-0000-4000-8000-000000000018"
    checkpoint = tmp_path / "checkpoint"
    stream = session.readStream.text(str(source))

    def interrupted(batch, batch_id):
        publish_raw(decode_chunks(batch, references), store, identity, batch_id)
        raise RuntimeError("owned interruption after commit before acknowledgment")

    query = (
        stream.writeStream.foreachBatch(interrupted)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(StreamingQueryException, match="interruption"):
        query.awaitTermination(60)
    query.stop()
    before = {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}

    def retry(batch, batch_id):
        publish_raw(decode_chunks(batch, references), store, identity, batch_id)

    resumed = (
        stream.writeStream.foreachBatch(retry)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    assert resumed.awaitTermination(60)
    resumed.stop()
    assert committed_rows(session, store, identity).count() == 4
    assert before == {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}
    assert (checkpoint / "commits/0").exists()


def test_transition_identity_matches_independent_preimage(rpki_session, rpki_fixture):
    from hashlib import sha256

    from schemas.temporal.identity import logical_identity

    _, _, _, _, references = rpki_fixture
    first, second = references.values()
    policy_preimage = (
        '["configstream-temporal-v1","policy_bindings","v1",'
        '[["string","rpki-csv-v1"],["string","membership-v1"],'
        '["string","csv-data-rows-v1"]]]'
    )
    policy = sha256(policy_preimage.encode()).hexdigest()
    member_id = "a" * 64
    expected = sha256(
        json.dumps(
            [
                "configstream-temporal-v1",
                "rpki_transition",
                policy,
                [
                    ["string", value]
                    for value in (
                        second.manifest.source_id,
                        second.manifest.source_scope,
                        first.observation_id,
                        second.observation_id,
                        member_id,
                        "add",
                        "consecutive-scope-set-v1",
                    )
                ],
            ],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    result = transition_ids(
        rpki_session.createDataFrame([(member_id, "add")], "membership_id STRING, action STRING"),
        first,
        second,
    ).first()
    assert result.logical_record_id == expected
    assert (
        logical_identity(
            "rpki_transition",
            policy,
            (
                second.manifest.source_id,
                second.manifest.source_scope,
                first.observation_id,
                second.observation_id,
                member_id,
                "add",
                "consecutive-scope-set-v1",
            ),
        )
        == expected
    )


def test_processor_rejects_plausible_wrong_source_mapping_before_raw_commit(
    rpki_session, rpki_fixture, tmp_path
):
    from schemas.rpki import chunk_identity
    from schemas.temporal.records import Authorization
    from spark.rpki_stream import RpkiProcessor

    session = rpki_session
    cache, selection, output, _, references = rpki_fixture
    summary = run_oracle(selection, cache, output)
    reference = next(iter(references.values()))
    original = next(
        source_chunks(cache / selection["artifacts"][0]["file"], reference, chunk_size=2)
    )
    rows = (
        original.rows[0].model_copy(
            update={
                "member": Authorization(
                    scope="owned.tal", asn=64503, prefix="203.0.113.0/24", max_length=24
                )
            }
        ),
        original.rows[1],
    )
    changed = original.model_copy(
        update={"rows": rows, "chunk_id": chunk_identity(reference, 1, rows)}
    )
    store = LocalStore(tmp_path / "lake")
    query = "00000000-0000-4000-8000-000000000020"
    processor = RpkiProcessor(session, store, query, references, output, summary["boundaries"])
    with pytest.raises(ValueError, match="coordinate oracle"):
        processor(session.createDataFrame([(changed.model_dump_json(),)], "value STRING"), 0)
    assert store.list(f"raw/{query}") == []
    assert store.list("state") == []
    assert store.list(f"quarantine/{query}")[0]["input_chunks"] == 1
