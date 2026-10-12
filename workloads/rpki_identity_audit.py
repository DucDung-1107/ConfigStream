"""Independent bounded transition identity audit over the existing SQLite oracle."""

import json
from datetime import date, timedelta
from hashlib import sha256

LIMIT = 20000
FIELDS = ("logical_record_id", "action", "member_encoding", "previous_id", "current_id")


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def identity(kind, policy, components):
    tagged = []
    for value in components:
        if type(value) is int:
            tagged.append(["integer", value])
        elif isinstance(value, str) and value.isascii() and len(value) <= 256:
            tagged.append(["string", value])
        else:
            raise ValueError("Independent RPKI audit accepts bounded ASCII/integer components")
    return sha256(encoded(["configstream-temporal-v1", kind, policy, tagged])).hexdigest()


def transition_ledger(database, previous, current):
    old = previous["observation"]["payload"]["observation_date"]
    new = current["observation"]["payload"]["observation_date"]
    if date.fromisoformat(new) - date.fromisoformat(old) != timedelta(days=1):
        raise ValueError("Independent identity audit cannot bridge a gap")
    manifest = current["manifest"]
    for name in (
        "source_id",
        "dataset_id",
        "source_scope",
        "parser_version",
        "canonicalization_version",
        "counting_policy_version",
    ):
        if previous["manifest"][name] != manifest[name]:
            raise ValueError("Independent identity audit policy/scope mismatch")
    comparison = current["observation"]["payload"]["comparison_policy"]
    if (
        previous["observation"]["payload"]["validation_regime"]
        != current["observation"]["payload"]["validation_regime"]
    ):
        raise ValueError("Independent identity audit validation mismatch")
    if previous["observation"]["payload"]["comparison_policy"] != comparison:
        raise ValueError("Independent identity audit comparison mismatch")
    policy = identity(
        "policy_bindings",
        "v1",
        [
            manifest["parser_version"],
            manifest["canonicalization_version"],
            manifest["counting_policy_version"],
        ],
    )
    result = []
    difference = (
        "SELECT encoding FROM members WHERE day=? EXCEPT SELECT encoding FROM members WHERE day=?"
    )
    for action, first, second in (("add", new, old), ("remove", old, new)):
        for (raw,) in database.execute(difference, (first, second)):
            if len(result) >= LIMIT:
                raise ValueError("Independent transition audit budget exceeded")
            scope, asn, prefix, maximum = json.loads(raw)
            if scope != manifest["source_scope"]:
                raise ValueError("Independent transition tuple scope mismatch")
            member_id = identity("rpki_membership", "membership-v1", [scope, asn, prefix, maximum])
            logical_id = identity(
                "rpki_transition",
                policy,
                [
                    manifest["source_id"],
                    scope,
                    previous["observation"]["envelope"]["logical_record_id"],
                    current["observation"]["envelope"]["logical_record_id"],
                    member_id,
                    action,
                    comparison,
                ],
            )
            result.append(
                dict(
                    logical_record_id=logical_id,
                    action=action,
                    member_encoding=raw.decode("utf-8"),
                    previous_id=previous["observation"]["envelope"]["logical_record_id"],
                    current_id=current["observation"]["envelope"]["logical_record_id"],
                )
            )
    return result


def validate_transition_ledger(expected, actual):
    projections = []
    for records in (expected, actual):
        if len(records) > LIMIT:
            raise ValueError("Independent transition audit budget exceeded")
        rows = [tuple(record[name] for name in FIELDS) for record in records]
        if len(rows) != len(set(rows)):
            raise ValueError("Duplicate transition occurrence in identity audit")
        projections.append(set(rows))
    if projections[0] != projections[1]:
        raise ValueError("Transition identities disagree with independent SQL ledger")
    digest = sha256(b"rpki-rows-v1\n")
    for row in sorted(projections[0], key=lambda value: value[0]):
        payload = encoded(list(row[:3]))
        digest.update(len(payload).to_bytes(4, "big"))
        digest.update(payload)
    return {
        "rows": len(projections[0]),
        "missing": 0,
        "unexpected": 0,
        "identity_digest": digest.hexdigest(),
    }
