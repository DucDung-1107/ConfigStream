import zipfile

from workloads.feasibility_artifacts import inspect_csv, inspect_zip
from workloads.feasibility_dns import address_observation


def test_csv_counts_quoted_rows_and_duplicate_repository_ids():
    result = inspect_csv(
        b"Name,Source URL\none,https://github.com/Owner/Repo\n"
        b'"two\nlines",https://github.com/owner/repo.git\n'
    )
    assert result["rows"] == 2
    assert result["distinct_github_origins"] == 1


def test_headerless_logs_do_not_publish_first_payload_as_schema():
    result = inspect_csv(b"FAKE_SENSITIVE_MARKER,value\nsecond,row\n", has_header=False)
    assert result["rows"] == 2
    assert result["columns"] == ["column_0", "column_1"]
    assert "FAKE_SENSITIVE_MARKER" not in repr(result)


def test_archive_crc_and_metadata_only_inspection(tmp_path):
    path = tmp_path / "fixture.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("data.csv", "name,version\nfixture,1\n")
        archive.writestr("untrusted.py", "raise RuntimeError('must not run')")
    result = inspect_zip(path)
    assert result["crc_verified"]
    assert result["csv"]["data.csv"]["rows"] == 1
    assert "must not run" not in repr(result)


def test_dns_key_ignores_ttl_and_worker_and_normalizes_names():
    row = {
        "query_name": "EXAMPLE.TEST.",
        "query_type": "A",
        "response_name": "Example.Test.",
        "response_type": "A",
        "status_code": 0,
        "ip4_address": "192.0.2.1",
        "response_ttl": 30,
        "worker_id": 1,
    }
    expected = (("example.test", "A", "example.test", "A"), "192.0.2.1")
    assert address_observation(row) == expected
    assert address_observation({**row, "response_ttl": 2, "worker_id": 7}) == expected
    assert address_observation({**row, "status_code": 65535}) is None
    assert address_observation({**row, "response_type": "TXT", "txt_text": "FAKE_SECRET"}) is None


def test_gzip_csv_archive_schema_not_raw_values(tmp_path):
    import gzip

    path = tmp_path / "fixture.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("versions.csv.gz", gzip.compress(b"id,version\nfixture,1\nfixture,2\n"))
    assert inspect_zip(path)["csv"]["versions.csv.gz"]["rows"] == 2


def test_mrt_sticky_error_does_not_drop_later_records(tmp_path, monkeypatch):
    import gzip
    import sys
    from types import SimpleNamespace

    from workloads.feasibility import bgp_probe

    class Reader:
        def __init__(self, path):
            self.err = None
            self.err_msg = "fixture parse error"
            self.data = {}

        def __iter__(self):
            self.err = True
            yield self
            self.data = {
                "timestamp": {1788220800: "ignored local rendering"},
                "peer_as": "64496",
                "peer_ip": "192.0.2.1",
                "bgp_message": {
                    "type": {2: "UPDATE"},
                    "nlri": [{"prefix": "198.51.100.0", "length": 24}],
                },
            }
            yield self

    def fake_fetch(url, path):
        path.write_bytes(gzip.compress(b"fixture"))
        return {"artifact_bytes": path.stat().st_size, "url": url}

    monkeypatch.setitem(sys.modules, "mrtparse", SimpleNamespace(Reader=Reader))
    monkeypatch.setattr("workloads.feasibility.fetch", fake_fetch)
    result = bgp_probe(tmp_path, tmp_path / "result.json")
    assert result["parse_errors"] == 12
    assert result["prefix_events"] == 12
    assert result["parse_error_categories"] == {"fixture parse error": 12}
