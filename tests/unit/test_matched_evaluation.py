import sqlite3
from hashlib import sha256

import pytest

from benchmarks.phase20_matched import ExactRows, canonical, digest_rows


def validator(tmp_path, rows):
    path = tmp_path / "expected.sqlite"
    database = sqlite3.connect(path)
    database.execute("CREATE TABLE expected(identity TEXT PRIMARY KEY,payload TEXT)")
    database.executemany("INSERT INTO expected VALUES (?,?)", rows)
    database.commit()
    database.close()
    return ExactRows(path, tmp_path / "seen.sqlite")


def test_full_projection_multiplicity_and_retry(tmp_path):
    payload = canonical({"value": "same", "nullable": None, "coordinate": 1})
    comparison = validator(tmp_path, [("source1", payload), ("source2", payload)])
    try:
        for identity in ["source2", "source1", "source1"]:
            comparison.accept(identity, payload)
        result = comparison.complete()
        assert result["logical_rows"] == 2
        assert result["physical_rows"] == 3
        assert result["reconciled_retries"] == 1
    finally:
        comparison.close()


@pytest.mark.parametrize("identity,payload", [("extra", "{}"), ("known", '{"changed":true}')])
def test_extra_and_conflicting_full_payload(tmp_path, identity, payload):
    comparison = validator(tmp_path, [("known", "{}")])
    try:
        with pytest.raises(ValueError, match="Unexpected identity or conflicting"):
            comparison.accept(identity, payload)
    finally:
        comparison.close()


def test_missing_last_identity_rejected(tmp_path):
    comparison = validator(tmp_path, [("first", "{}"), ("last", "{}")])
    try:
        comparison.accept("first", "{}")
        with pytest.raises(ValueError, match="Missing"):
            comparison.complete()
    finally:
        comparison.close()


def test_checksum_fixed_vector_and_no_nan():
    expected = sha256(b"phase20-core-v1\n\x00\x00\x00\x01a\x00\x00\x00\x02{}").hexdigest()
    assert digest_rows([("a", "{}")]) == expected
    assert canonical({"time": 123, "unknown": None}) == '{"time":123,"unknown":null}'
    with pytest.raises(ValueError):
        canonical({"time": float("nan")})
