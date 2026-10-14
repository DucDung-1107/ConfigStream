import json
from hashlib import sha256
from pathlib import Path

import pytest

from dashboard.export import Catalog

ROOT = Path(__file__).resolve().parents[2]


def bundle():
    return json.loads((ROOT / "frontend/public/research/v1/catalog.json").read_text())


def test_dashboard_binds_every_approved_source_artifact():
    data = bundle()
    bindings = json.loads((ROOT / "dashboard/source-bindings.json").read_text())
    for relative, reference in data["artifacts"].items():
        assert reference["sha256"] == bindings[relative]
        assert sha256((ROOT / relative).read_bytes()).hexdigest() == reference["sha256"]
    assert data["ris_summary"]["occurrences"] == sum(
        row["occurrences"] for row in data["ris_minutes"]
    )


@pytest.mark.parametrize(
    "fault",
    ["partial_day", "missing_minute", "promoted_h3", "matched_vantage", "collapsed_occurrence"],
)
def test_dashboard_unsafe_semantics_fail_closed(fault):
    data = bundle()
    if fault == "partial_day":
        data["rpki_days"][0]["status"] = "PARTIAL"
    elif fault == "missing_minute":
        data["ris_minutes"].pop()
    elif fault == "promoted_h3":
        data["hypotheses"]["H3"]["decision"] = "SUPPORTED_WITHIN_TESTED_SCOPE"
    elif fault == "matched_vantage":
        data["dns_comparison"]["strict_matched_vantage_eligible"] = 1
    else:
        data["ris_fixture"][1]["id"] = data["ris_fixture"][0]["id"]
    with pytest.raises(ValueError):
        Catalog.model_validate(data)


def test_dashboard_privacy_and_native_units():
    data = Catalog.model_validate(bundle()).model_dump()
    assert [row["count"] for row in data["domains"]] == [10188, 14, 4725156, 2234863]
    assert len({row["unit"] for row in data["domains"]}) == 4
    assert len({row["fingerprint"] for row in data["ris_fixture"]}) == 2
    assert all(row["evidence"] == "PROJECT-OWNED DEMO FIXTURE" for row in data["ris_fixture"])
    raw = json.dumps(data).lower()
    assert all(
        token not in raw
        for token in ("minio_secret", "kubeconfig", "query_name", "kafka_payload", "access_key")
    )
