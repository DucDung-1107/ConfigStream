from itertools import permutations
from random import Random

import pytest

from processing import NormalizedConfig
from processing.errors import AssetMismatchError
from processing.state import AssetVersionState, BufferedSnapshot, accept_snapshot, expire_state


def buffered(version: int, asset: str = "asset-A", identity: str | None = None) -> BufferedSnapshot:
    return BufferedSnapshot(
        NormalizedConfig(asset, identity or f"{asset}-{version}", version, f"timeout {version}\n"),
        1_000_000 * version,
    )


@pytest.mark.parametrize("order", list(permutations((1, 2, 3))) + [(1, 2), (2, 3), (7, 8, 6)])
def test_adjacent_pairs_for_every_arrival_order(order) -> None:
    state = AssetVersionState("asset-A")
    pairs = []
    for version in order:
        result = accept_snapshot(state, buffered(version), 32)
        state = result.state
        pairs.extend((old.version, new.version) for old, new in result.pairs)
    expected = {(version, version + 1) for version in order if version + 1 in order}
    assert set(pairs) == expected
    assert len(pairs) == len(expected)


@pytest.mark.parametrize("seed", range(100))
def test_permuted_replays_preserve_pair_uniqueness(seed: int) -> None:
    random = Random(seed)
    order = list(range(1, 21)) * 2
    random.shuffle(order)
    state = AssetVersionState("asset-A")
    pairs = []
    for version in order:
        result = accept_snapshot(state, buffered(version), 32)
        state = result.state
        pairs.extend((old.version, new.version) for old, new in result.pairs)
    assert sorted(pairs) == [(version, version + 1) for version in range(1, 20)]
    assert len(state.snapshots) == 20
    assert len(state.emitted) == 19


def test_gap_held_and_reported_at_timeout() -> None:
    state = AssetVersionState("asset-A")
    for version in (1, 3):
        result = accept_snapshot(state, buffered(version), 32)
        assert result.pairs == ()
        state = result.state
    assert expire_state(state, 32_999, 30_000).state == state
    expired = expire_state(state, 33_000, 30_000)
    assert expired.state == AssetVersionState("asset-A")
    gap, timeout = expired.diagnostics
    assert gap.reason == "unresolved_version_gap"
    assert (gap.versions, gap.missing_start, gap.missing_end) == ((1, 3), 2, 2)
    assert timeout.reason == "state_timeout"
    assert expire_state(expired.state, 99_000, 30_000).diagnostics == ()


def test_conflict_is_not_duplicate_and_first_wins() -> None:
    state = accept_snapshot(AssetVersionState("asset-A"), buffered(2), 32).state
    assert accept_snapshot(state, buffered(2), 32).diagnostics == ()
    conflict = accept_snapshot(state, buffered(2, identity="conflict"), 32)
    assert conflict.state == state
    assert conflict.diagnostics[0].reason == "version_conflict"
    result = accept_snapshot(conflict.state, buffered(3), 32)
    assert result.pairs[0][0].config.snapshot_id == "asset-A-2"


def test_interleaved_assets_are_isolated() -> None:
    states = {asset: AssetVersionState(asset) for asset in ("A", "B")}
    pairs = []
    for asset, version in (("A", 1), ("B", 10), ("A", 2), ("B", 11)):
        result = accept_snapshot(states[asset], buffered(version, asset), 32)
        states[asset] = result.state
        pairs.extend(result.pairs)
    assert [(old.config.asset_id, old.version, new.version) for old, new in pairs] == [
        ("A", 1, 2),
        ("B", 10, 11),
    ]
    with pytest.raises(AssetMismatchError):
        accept_snapshot(states["A"], buffered(3, "B"), 32)


def test_eviction_floor_prevents_pair_reemission() -> None:
    state = AssetVersionState("asset-A")
    for version in (1, 3, 4):
        result = accept_snapshot(state, buffered(version), 2)
        state = result.state
    assert [item.version for item in state.snapshots] == [3, 4]
    assert state.floor == 1
    assert {item.reason for item in result.diagnostics} == {
        "state_eviction",
        "unresolved_version_gap",
    }
    replay = accept_snapshot(state, buffered(1), 2)
    assert replay.diagnostics[0].reason == "evicted_version"
    assert replay.pairs == ()
    late = accept_snapshot(state, buffered(2), 2)
    assert [(old.version, new.version) for old, new in late.pairs] == [(2, 3)]
    assert late.state.floor == 2
    assert accept_snapshot(late.state, buffered(2), 2).pairs == ()


@pytest.mark.parametrize("limit", [2, 3, 8, 32])
def test_long_history_is_bounded(limit: int) -> None:
    state = AssetVersionState("asset-A")
    pairs = []
    for version in range(1, 201):
        result = accept_snapshot(state, buffered(version), limit)
        state = result.state
        assert len(state.snapshots) <= limit
        assert len(state.emitted) <= limit - 1
        pairs.extend(result.pairs)
    assert len(pairs) == 199


@pytest.mark.parametrize("limit", [0, 1, -1, True, 2.5])
def test_invalid_cap(limit) -> None:
    with pytest.raises(ValueError):
        accept_snapshot(AssetVersionState("asset-A"), buffered(1), limit)
