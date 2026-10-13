import hashlib

import pytest

from benchmarks.evaluation import (
    encoding_digest,
    exact_encoding_digest,
    preference,
    randomized_order,
    statistics,
)


def test_order_repetitions():
    order = randomized_order(["R1", "R2", "R3"])
    assert order == randomized_order(["R1", "R2", "R3"])
    assert len(order) == len(set(order)) == 9
    with pytest.raises(ValueError):
        randomized_order(["R1"], 2)


def test_preference_every_required_query_and_thresholds():
    baseline = {"reconstruct": [1, 2, 3], "membership": [1, 1, 1]}
    assert preference(100, 80, baseline, baseline, correct=True)["status"] == "PREFERRED"
    assert preference(100, 81, baseline, baseline, correct=True)["status"] == "NOT_PREFERRED"
    slow = baseline | {"membership": [3, 3, 3]}
    assert preference(100, 50, baseline, slow, correct=True)["status"] == "NOT_PREFERRED"
    assert preference(100, 50, baseline, {}, correct=True)["status"] == "INCONCLUSIVE"
    assert (
        preference(100, 50, baseline, baseline, correct=False)["status"] == "REJECTED_CORRECTNESS"
    )
    assert (
        preference(100, 50, {"q": [1]}, {"q": [1]}, correct=True)["status"]
        == "INSUFFICIENT_REPETITIONS"
    )


def test_fixed_checksum_and_exact_difference():
    expected = hashlib.sha256(b"rpki-set-v1\n\x00\x00\x00\x01a\x00\x00\x00\x01b").hexdigest()
    assert encoding_digest([(b"a",), (b"b",)]) == dict(rows=2, set_digest=expected)
    assert exact_encoding_digest([(b"a",), (b"b",)], [(b"a",), (b"b",)]) == dict(
        rows=2, set_digest=expected
    )
    for actual in [[(b"a",)], [(b"a",), (b"c",)], [(b"a",), (b"b",), (b"c",)]]:
        with pytest.raises(ValueError):
            exact_encoding_digest(actual, [(b"a",), (b"b",)])


def test_statistics_retains_slow_run():
    assert statistics([3, 1, 8]) == dict(individual=[3, 1, 8], median=3, minimum=1, maximum=8)
    with pytest.raises(ValueError):
        statistics([])


@pytest.mark.parametrize("invalid", [-1, float("nan"), float("inf")])
def test_nonfinite_or_negative_measurements_fail_closed(invalid):
    with pytest.raises(ValueError):
        statistics([1, invalid, 2])
    with pytest.raises(ValueError):
        preference(100, 80, {"q": [1, 1, 1]}, {"q": [1, invalid, 1]}, correct=True)


def test_unsafe_payload_dedup_is_falsified():
    owned = {"coordinate1": "same-payload", "coordinate2": "same-payload"}
    unsafe = {payload for payload in owned.values()}
    assert len(unsafe) == 1 and len(owned) == 2


@pytest.mark.parametrize("mode", ["R1", "R2", "R3"])
def test_owned_fourteen_day_representations(mode, tmp_path):
    import json
    import sqlite3

    pytest.importorskip("pyarrow")
    from benchmarks.phase20 import build_rpki, rpki_queries

    oracle = sqlite3.connect(":memory:")
    oracle.execute("CREATE TABLE members(day TEXT, encoding BLOB, PRIMARY KEY(day,encoding))")
    days = [f"2024-01-{index:02d}" for index in range(1, 15)]
    for index, day in enumerate(days):
        values = [b'["owned",64496,"192.0.2.0/24",24]']
        if index % 2:
            values.append(b'["owned",64497,"198.51.100.0/24",24]')
        oracle.executemany("INSERT INTO members VALUES (?,?)", [(day, value) for value in values])
    build_rpki(mode, oracle, days, tmp_path, lambda: None)
    results = rpki_queries(mode, oracle, days, tmp_path, 1, lambda: None)
    assert len(results) == 5 and all(item["correctness"] == "PASS" for item in results)
    assert all(item["seconds"] >= 0 and item["validation_seconds"] >= 0 for item in results)
    assert all(
        item["total_seconds"] == pytest.approx(item["seconds"] + item["validation_seconds"])
        for item in results
    )
    assert all(len(item["output_sha256"]) == 64 for item in results)
    if mode == "R2":
        import pyarrow as arrow
        import pyarrow.parquet as parquet

        parquet.write_table(
            arrow.table({"member_encoding": [json.dumps(["owned", 64498, "203.0.113.0/24", 24])]}),
            tmp_path / "removed-1.parquet",
        )
        with pytest.raises(ValueError, match="nonexistent"):
            rpki_queries(mode, oracle, days, tmp_path, 2, lambda: None)


def test_guard_sampling_failure_propagates(tmp_path, monkeypatch):
    from benchmarks.phase20 import guards

    def unavailable():
        raise OSError("owned unavailable guest sample")

    monkeypatch.setattr("benchmarks.rpki_service.guest_resources", unavailable)
    with pytest.raises(OSError):
        with guards(tmp_path, 30):
            pytest.fail("Must not execute after failed preflight")


def test_report_refuses_incomplete_or_incorrect_trials(tmp_path):
    import json

    from benchmarks.phase20_report import load_trials

    with pytest.raises(ValueError):
        load_trials(tmp_path)
    for repetition in range(1, 4):
        for mode in ("R1", "R2", "R3"):
            output = tmp_path / f"{mode}-{repetition}"
            output.mkdir()
            (output / "result.json").write_text(
                json.dumps(
                    dict(
                        mode=mode, repetition=repetition, status="FAIL" if mode == "R3" else "PASS"
                    )
                )
            )
    with pytest.raises(ValueError):
        load_trials(tmp_path)


def test_report_cannot_hide_missing_required_query(tmp_path):
    import json

    from benchmarks.phase20_report import summarize_rpki

    for repetition in range(1, 4):
        for mode in ("R1", "R2", "R3"):
            output = tmp_path / f"{mode}-{repetition}"
            output.mkdir()
            (output / "result.json").write_text(
                json.dumps(dict(mode=mode, repetition=repetition, status="PASS", queries=[]))
            )
    with pytest.raises(ValueError, match="Required query"):
        summarize_rpki(tmp_path)
