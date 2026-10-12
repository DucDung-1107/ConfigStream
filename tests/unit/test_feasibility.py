import json
from datetime import UTC, datetime

import pytest

from workloads.feasibility import (
    digest,
    fetch,
    missing_dates,
    mrt_events,
    parse_vrps,
    state_diff,
    utc_timestamp,
    write_json,
)


def test_vrp_duplicates_and_identity():
    content = (
        b"ASN,IP Prefix,Max Length,Trust Anchor\n"
        b"AS64496,192.0.2.0/24,24,ignored\n"
        b"AS64496,192.0.2.0/24,24,ignored\n"
        b"AS64496,2001:db8::/32,48,ignored\n"
    )
    values, metadata = parse_vrps(content, "fixture")
    assert values == {
        ("fixture", 64496, "192.0.2.0/24", 24),
        ("fixture", 64496, "2001:db8::/32", 48),
    }
    assert metadata["rows"] == 3
    assert metadata["duplicates"] == 1


@pytest.mark.parametrize(
    "content",
    [
        "unknown,columns\n1,2\n",
        "ASN,IP Prefix,Max Length\nAS64496,192.0.2.0/24,23\n",
        "ASN,IP Prefix,Max Length\nAS64496,192.0.2.1/24,24\n",
        "ASN,IP Prefix,Max Length\nAS4294967296,192.0.2.0/24,24\n",
    ],
)
def test_invalid_vrp_rejected(content):
    with pytest.raises(ValueError):
        parse_vrps(content.encode(), "fixture")


def test_unchanged_state_is_not_a_change():
    assert state_diff({1, 2}, {1, 2}) == {
        "added": 0,
        "removed": 0,
        "unchanged": 2,
        "change_ratio_union": 0,
    }
    assert state_diff({1, 2}, {2, 3}) == {
        "added": 1,
        "removed": 1,
        "unchanged": 1,
        "change_ratio_union": 2 / 3,
    }
    assert state_diff(set(), set())["change_ratio_union"] == 0


def test_missing_dates_does_not_bridge_a_gap():
    assert missing_dates({"2026-09-01", "2026-09-03"}, "2026-09-01", 3) == ["2026-09-02"]


@pytest.mark.parametrize(
    "value,milliseconds",
    [
        ("2026-09-01T07:00:00+07:00", False),
        (1788220800, False),
        (1788220800000, True),
    ],
)
def test_utc_conversion(value, milliseconds):
    assert utc_timestamp(value, milliseconds=milliseconds) == datetime(2026, 9, 1, tzinfo=UTC)


def test_naive_time_is_rejected():
    with pytest.raises(ValueError):
        utc_timestamp("2026-09-01T00:00:00")


def test_mrt_events_are_prefix_events_not_message_count():
    record = {
        "peer_as": "64496",
        "peer_ip": "192.0.2.1",
        "bgp_message": {
            "type": {2: "UPDATE"},
            "nlri": [{"prefix": "198.51.100.0", "length": 24}],
            "withdrawn_routes": [{"prefix": "203.0.113.0", "length": 24}],
            "path_attributes": [
                {
                    "type": {14: "MP_REACH_NLRI"},
                    "value": {
                        "safi": {1: "UNICAST"},
                        "nlri": [{"prefix": "2001:db8::", "length": 32, "path_id": 7}],
                    },
                }
            ],
        },
    }
    events = mrt_events(record)
    assert [value[0] for value in events] == ["withdrawal", "announcement", "announcement"]
    assert events[-1][-1] == 7
    assert mrt_events({"bgp_message": {"type": {4: "KEEPALIVE"}}}) == []


def test_mrt_rejects_unsupported_address_family():
    record = {
        "peer_as": "64496",
        "peer_ip": "192.0.2.1",
        "bgp_message": {
            "type": {2: "UPDATE"},
            "path_attributes": [{"type": {14: "MP_REACH_NLRI"}, "value": {"safi": {128: "VPN"}}}],
        },
    }
    with pytest.raises(ValueError):
        mrt_events(record)


def test_atomic_deterministic_summary_and_digest(tmp_path):
    path = tmp_path / "result.json"
    write_json(path, {"schema_version": 1, "missing": None, "records": 3})
    previous = digest(path)
    write_json(path, {"records": 3, "missing": None, "schema_version": 1})
    assert digest(path) == previous
    assert json.loads(path.read_text())["missing"] is None
    assert not list(tmp_path.glob("*.tmp"))


def test_cached_checksum_validation_and_budget(tmp_path):
    path = tmp_path / "fixture.txt"
    path.write_text("safe fixture")
    url = "https://ftp.ripe.net/rpki/fixture.txt"
    assert fetch(url, path, expected_sha256=digest(path))["artifact_bytes"] == 12
    with pytest.raises(ValueError, match="checksum"):
        fetch(url, path, expected_sha256="0" * 64)
    with pytest.raises(ValueError, match="budget"):
        fetch(url, path, limit=1)


@pytest.mark.parametrize(
    "url",
    [
        "http://ftp.ripe.net/test",
        "https://evil.invalid/test",
        "https://user:password@ftp.ripe.net/test",
    ],
)
def test_fetch_rejects_unsafe_origin_without_network(tmp_path, url):
    with pytest.raises(ValueError):
        fetch(url, tmp_path / "result")


def test_disk_guard_runs_before_network(tmp_path, monkeypatch):
    class Usage:
        free = 1

    monkeypatch.setattr("workloads.feasibility.shutil.disk_usage", lambda path: Usage())
    with pytest.raises(OSError, match="reserve"):
        fetch("https://ftp.ripe.net/test", tmp_path / "result")
