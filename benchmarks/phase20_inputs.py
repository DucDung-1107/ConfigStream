"""Read-only accepted normalized-input binding; no source parsing or publication."""

import hashlib
import json
import sqlite3
from pathlib import Path
from time import monotonic

from benchmarks.phase20 import guards
from workloads.rpki_oracle import independent_member_id, set_checksum


def audit(check):
    import pyarrow.parquet as parquet

    days = json.loads(Path("workloads/results/phase17/oracle-daily-states.json").read_text())[
        "days"
    ]
    database = sqlite3.connect("file:.cache/phase17/oracle/oracle.sqlite?mode=ro", uri=True)
    verified = []
    for day in days:
        check()
        actual = set_checksum(
            database.execute(
                "SELECT encoding FROM members WHERE day=? ORDER BY encoding", (day["date"],)
            )
        )
        if actual != day["set_digest"]:
            raise ValueError("RPKI normalized tuple oracle differs from public accepted evidence")
        checksum = hashlib.sha256(b"rpki-source-map-v1\n")
        count = 0
        source = Path(".cache/phase17/oracle/source_maps") / (day["date"] + ".parquet")
        for batch in parquet.ParquetFile(source).iter_batches(batch_size=8192):
            check()
            for row in batch.to_pylist():
                count += 1
                if row["row_index"] != count:
                    raise ValueError("Missing/repeated original RPKI source coordinate")
                encoded = json.dumps(
                    [row["row_index"], row["member_encoding"]],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
                checksum.update(len(encoded).to_bytes(4, "big"))
                checksum.update(encoded)
        if checksum.hexdigest() != day["source_map_digest"] or count != day["source_rows"]:
            raise ValueError("RPKI source accounting changed")
        verified.append(
            dict(
                day=day["date"],
                set_digest=actual,
                source_rows=count,
                source_map_digest=checksum.hexdigest(),
            )
        )
    database.close()
    checksum = hashlib.sha256(b"rpki-set-v1\n")
    members = 0
    for batch in parquet.ParquetFile(
        ".cache/phase17/oracle/members/2026-09-01.parquet"
    ).iter_batches(batch_size=8192):
        check()
        for row in batch.to_pylist():
            fields = json.loads(row["member_encoding"])
            if fields != [row[name] for name in ("scope", "asn", "prefix", "max_length")] or row[
                "membership_id"
            ] != independent_member_id(*fields):
                raise ValueError("RPKI six-field pilot projection changed")
            encoded = row["member_encoding"].encode()
            checksum.update(len(encoded).to_bytes(4, "big"))
            checksum.update(encoded)
            members += 1
    if checksum.hexdigest() != days[0]["set_digest"] or members != days[0]["unique_members"]:
        raise ValueError("RPKI pilot member projection incomplete")
    ris = json.loads(
        Path("workloads/results/phase18-closure/occurrence-ledger-validation.json").read_text()
    )["archives"][0]
    dns = json.loads(Path("workloads/results/phase19/independent-oracle.json").read_text())[
        "sources"
    ][0]["summary"]
    projections = []
    for kind, path, expected, expected_count in [
        (
            "ris",
            ".cache/phase18-closure/oracle-hour-v2/updates.20260901.0000.gz/events.parquet",
            ris["source_order_projection_sha256"],
            ris["accepted_occurrences"],
        ),
        (
            "dns",
            ".cache/phase19/oracle/2026-09-01/raw.parquet",
            dns["source_order_sha256"],
            dns["source_rows"],
        ),
    ]:
        checksum = hashlib.sha256()
        count = 0
        for batch in parquet.ParquetFile(path).iter_batches(batch_size=8192):
            check()
            for row in batch.to_pylist():
                count += 1
                checksum.update(
                    json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                )
        if checksum.hexdigest() != expected or count != expected_count:
            raise ValueError("Normalized pilot input differs from accepted ledger")
        projections.append(dict(workload=kind, rows=count, source_order_sha256=expected))
    return dict(
        rpki=verified,
        pilot_projections=projections,
        status="PASS",
        evidence="MEASURED_READ_ONLY_ACCEPTED_INPUT_BINDING",
        mutation=False,
    )


if __name__ == "__main__":
    output = Path(".cache/phase20/normalized-audit")
    output.mkdir(exist_ok=False)
    started = monotonic()
    with guards(output, 600) as check:
        result = audit(check)
    result["elapsed_seconds"] = monotonic() - started
    print(json.dumps(result, sort_keys=True))
