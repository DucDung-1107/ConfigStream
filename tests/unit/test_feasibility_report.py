import copy
import json
from pathlib import Path

import pytest

from workloads.feasibility_artifacts import inspect_csv
from workloads.feasibility_dns import address_observation
from workloads.feasibility_report import SPECIFICATION, complementarity, report, validate_registry


def test_all_candidates_have_evidence_and_bounded_scores():
    candidates = validate_registry(json.loads(SPECIFICATION.read_text()))
    assert len(candidates) == 23
    assert all(candidate["sources"] for candidate in candidates)


@pytest.mark.parametrize("mutation", ["duplicate", "access", "score"])
def test_invalid_registry_is_rejected(mutation):
    specification = json.loads(SPECIFICATION.read_text())
    if mutation == "duplicate":
        specification["candidates"][1] = copy.deepcopy(specification["candidates"][0])
    elif mutation == "access":
        specification["candidates"][0]["access"] = "ASSUMED_ACCESSIBLE"
    else:
        specification["candidates"][0]["scores"][0] = 21
    with pytest.raises(ValueError):
        validate_registry(specification)


def test_overlap_does_not_double_count_bgpstream():
    candidates = {
        row["id"]: row for row in validate_registry(json.loads(SPECIFICATION.read_text()))
    }
    assert complementarity(candidates["D1"], candidates["D3"]) == 0
    assert complementarity(candidates["D1"], candidates["D2"]) == 2
    assert complementarity(candidates["A2"], candidates["B1"]) == 8


def test_semicolon_metadata_and_empty_csv():
    assert inspect_csv(b"repository;version\nexample;v1\n")["columns"] == ["repository", "version"]
    assert inspect_csv(b"")["rows"] == 0


def test_ipv6_address_representation_is_canonical():
    row = {
        "query_name": "EXAMPLE.TEST.",
        "query_type": "AAAA",
        "response_name": "EXAMPLE.TEST.",
        "response_type": "AAAA",
        "status_code": 0,
        "ip6_address": "2001:0db8:0000:0000:0000:0000:0000:0001",
    }
    assert address_observation(row)[1] == "2001:db8::1"


def test_generated_report_is_deterministic(tmp_path):
    import shutil

    for name in ("rpki", "bgp", "dns", "artifacts"):
        shutil.copyfile(
            Path("workloads/results/dataset-feasibility") / (name + ".json"),
            tmp_path / (name + ".json"),
        )
    assert report(tmp_path)["candidates"] == 23
    first = {path.name: path.read_bytes() for path in tmp_path.glob("*.csv")}
    report(tmp_path)
    assert first == {path.name: path.read_bytes() for path in tmp_path.glob("*.csv")}
