"""Expected identities use manually written preimages, not the audit helper."""

import json
import sqlite3
from hashlib import sha256

import pytest

from workloads.rpki_identity_audit import identity, transition_ledger, validate_transition_ledger


def test_independent_member_vector_and_typed_components():
    preimage = (
        b'["configstream-temporal-v1","rpki_membership","membership-v1",'
        b'[["string","owned.tal"],["integer",64500],'
        b'["string","192.0.2.0/24"],["integer",24]]]'
    )
    assert (
        identity("rpki_membership", "membership-v1", ["owned.tal", 64500, "192.0.2.0/24", 24])
        == sha256(preimage).hexdigest()
    )
    with pytest.raises(ValueError, match="bounded ASCII/integer"):
        identity("owned", "v1", [True])


def test_sql_identity_audit_against_owned_source_and_negative_records(rpki_fixture):
    _, _, oracle, _, references = rpki_fixture
    first, second = [item.model_dump(mode="json") for item in references.values()]
    with sqlite3.connect(f"file:{oracle / 'oracle.sqlite'}?mode=ro", uri=True) as database:
        expected = transition_ledger(database, first, second)
    assert sorted(row["action"] for row in expected) == ["add", "remove"]
    policy = sha256(
        b'["configstream-temporal-v1","policy_bindings","v1",'
        b'[["string","rpki-csv-v1"],["string","membership-v1"],'
        b'["string","csv-data-rows-v1"]]]'
    ).hexdigest()
    for action, asn, prefix, maximum in (
        ("add", 64503, "203.0.113.0/24", 24),
        ("remove", 64502, "2001:db8::/32", 48),
    ):
        member_preimage = (
            '["configstream-temporal-v1","rpki_membership","membership-v1",'
            f'[["string","owned.tal"],["integer",{asn}],'
            f'["string","{prefix}"],["integer",{maximum}]]]'
        )
        components = [
            "ripe-rpki-archive",
            "owned.tal",
            first["observation"]["envelope"]["logical_record_id"],
            second["observation"]["envelope"]["logical_record_id"],
            sha256(member_preimage.encode()).hexdigest(),
            action,
            "consecutive-scope-set-v1",
        ]
        vector = [
            "configstream-temporal-v1",
            "rpki_transition",
            policy,
            [["string", component] for component in components],
        ]
        actual = next(row for row in expected if row["action"] == action)
        assert (
            actual["logical_record_id"]
            == sha256(json.dumps(vector, separators=(",", ":")).encode()).hexdigest()
        )
        assert actual["member_encoding"] == json.dumps(
            ["owned.tal", asn, prefix, maximum], separators=(",", ":")
        )
    assert validate_transition_ledger(expected, list(reversed(expected)))["rows"] == 2
    with pytest.raises(ValueError, match="disagree"):
        validate_transition_ledger(expected, expected[:1])
    with pytest.raises(ValueError, match="disagree"):
        validate_transition_ledger(
            expected, [expected[0] | {"logical_record_id": "f" * 64}, expected[1]]
        )
    with pytest.raises(ValueError, match="Duplicate"):
        validate_transition_ledger(expected, expected + [expected[0]])
    second["observation"]["payload"]["observation_date"] = "2026-09-04"
    with sqlite3.connect(f"file:{oracle / 'oracle.sqlite'}?mode=ro", uri=True) as database:
        with pytest.raises(ValueError, match="gap"):
            transition_ledger(database, first, second)
