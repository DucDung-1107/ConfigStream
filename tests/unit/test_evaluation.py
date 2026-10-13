import hashlib

import pytest

from benchmarks.evaluation import (
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


def test_unsafe_payload_dedup_is_falsified():
    owned = {"coordinate1": "same-payload", "coordinate2": "same-payload"}
    unsafe = {payload for payload in owned.values()}
    assert len(unsafe) == 1 and len(owned) == 2
