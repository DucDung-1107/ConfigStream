"""Independent PyArrow row-group / SQLite observation oracle; private outputs only."""

import argparse
import hashlib
import ipaddress
import itertools
import json
import shutil
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path
from time import monotonic

from schemas.dns import CANONICAL, COUNTING, PARSER, DnsReference, DnsRow
from schemas.temporal.provenance import Artifact, SourceManifest, verify_artifact_file


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def independent_id(kind, policy, components):
    tags = [["integer" if type(value) is int else "string", value] for value in components]
    return hashlib.sha256(
        encoded(["configstream-temporal-v1", kind, policy, tags]).encode()
    ).hexdigest()


def reference(path, artifact):
    import pyarrow.parquet as parquet

    verify_artifact_file(
        path,
        Artifact(
            sha256=artifact["sha256"],
            bytes=artifact["artifact_bytes"],
            safe_reference=artifact["url"],
            revision="phase13-pinned-object",
        ),
    )
    footer = parquet.ParquetFile(path).metadata
    return DnsReference(
        partition_date=date.fromisoformat(artifact["date"]),
        row_groups=tuple(
            footer.row_group(index).num_rows for index in range(footer.num_row_groups)
        ),
        manifest=SourceManifest(
            manifest_version="1.0",
            source_id="openintel",
            dataset_id="openintel-li-two-selected-objects",
            source_scope="li:zonefile:selected-objects",
            artifacts=(
                Artifact(
                    sha256=artifact["sha256"],
                    bytes=artifact["artifact_bytes"],
                    safe_reference=artifact["url"],
                    revision="phase13-pinned-object",
                ),
            ),
            parser_version=PARSER,
            canonicalization_version=CANONICAL,
            counting_policy_version=COUNTING,
            selection_manifest_id=hashlib.sha256(b"phase19-two-pinned-li-objects").hexdigest(),
            source_time_coverage={
                "kind": "unknown",
                "role": "observation",
                "reason": "individual recorded measurement timestamps retained separately",
            },
            declared_observation_coverage="Selected object only; entity/answer coverage unknown",
            license_terms_reference="https://openintel.nl/download/terms/",
            expected_records=footer.num_rows,
            accepted_records=footer.num_rows,
            excluded_records=0,
            integrity_verified=True,
            accounting_complete=True,
        ),
    )


def reference_row(raw, source, group, ordinal):
    status = raw["status_code"]
    outcome = {0: "success", 65535: "timeout"}.get(
        status, "unknown" if status is None or not 1 <= status <= 15 else "failure"
    )
    disposition = "unsuccessful" if outcome != "success" else "non_target"
    address = None
    if status == 0 and raw["response_type"] in ("A", "AAAA"):
        candidate = raw["ip4_address"] if raw["response_type"] == "A" else raw["ip6_address"]
        disposition = "unavailable"
        if candidate and all(raw[key] for key in ("query_name", "query_type", "response_name")):
            try:
                address = str(
                    (
                        ipaddress.IPv4Address
                        if raw["response_type"] == "A"
                        else ipaddress.IPv6Address
                    )(candidate)
                )
                disposition = "selected_address"
            except ipaddress.AddressValueError:
                disposition = "invalid_address"
    fields = {
        key: raw[key]
        for key in (
            "query_name",
            "query_type",
            "response_name",
            "response_type",
            "worker_id",
            "status_code",
            "ip4_address",
            "ip6_address",
            "response_ttl",
            "section",
            "extended_error",
        )
    }
    manifest = source.manifest
    return DnsRow.model_validate(
        fields
        | dict(
            logical_record_id=independent_id(
                "dns_measurement",
                source.policy,
                (
                    manifest.source_id,
                    manifest.source_scope,
                    manifest.artifacts[0].sha256,
                    group,
                    ordinal,
                ),
            ),
            artifact_sha256=manifest.artifacts[0].sha256,
            provenance_manifest_id=manifest.manifest_id,
            row_group=group,
            row=ordinal,
            partition_date=source.partition_date.isoformat(),
            timestamp_ms=raw["timestamp"],
            rtt_hex=float(raw["rtt"]).hex() if raw["rtt"] is not None else None,
            source_row_sha256=hashlib.sha256(encoded(raw).encode()).hexdigest(),
            outcome=outcome,
            disposition=disposition,
            address_value=address,
        )
    ).model_dump()


class Columnar:
    def __init__(self, path, schema):
        import pyarrow.parquet as parquet

        self.path, self.schema = path, schema
        self.writer = parquet.ParquetWriter(path, schema, compression="snappy")
        self.rows = []

    def append(self, row):
        self.rows.append(row)
        if len(self.rows) >= 8192:
            self.flush()

    def flush(self):
        import pyarrow as arrow

        if self.rows:
            self.writer.write_table(arrow.Table.from_pylist(self.rows, schema=self.schema))
            self.rows.clear()

    def close(self):
        self.flush()
        self.writer.close()


def raw_schema():
    import pyarrow as arrow

    integers = {"row_group", "row", "timestamp_ms", "worker_id", "status_code", "response_ttl"}
    return arrow.schema(
        [(key, arrow.int64() if key in integers else arrow.string()) for key in DnsRow.model_fields]
    )


def group_schema():
    import pyarrow as arrow

    integer = {
        "minimum_timestamp_ms",
        "maximum_timestamp_ms",
        "unknown_worker",
        "unknown_time",
        "contributors",
    }
    fields = [
        "group_id",
        "group_key",
        "artifact_sha256",
        "partition_date",
        "query_name",
        "query_type",
        "response_name",
        "response_type",
        "value_set_id",
        "addresses_json",
        "workers_json",
        "minimum_timestamp_ms",
        "maximum_timestamp_ms",
        "unknown_worker",
        "unknown_time",
        "contributors",
        "entity_coverage",
        "vantage",
    ]
    return arrow.schema(
        [(key, arrow.int64() if key in integer else arrow.string()) for key in fields]
    )


def build(path, source, output, *, check=lambda: None):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    if output.exists() or arrow.__version__ != "19.0.1":
        raise ValueError("Fresh DNS oracle output / pinned reader required")
    output.mkdir(parents=True)
    started = monotonic()
    verify_artifact_file(path, source.manifest.artifacts[0])
    database = sqlite3.connect(output / "oracle.sqlite")
    database.executescript(
        "PRAGMA journal_mode=OFF; PRAGMA cache_size=-8192;"
        "CREATE TABLE raw(identity TEXT PRIMARY KEY, data TEXT);"
        "CREATE TABLE addresses(group_key TEXT, address TEXT, identity TEXT,"
        "worker INTEGER, timestamp INTEGER); CREATE TABLE queries(name TEXT PRIMARY KEY);"
        "CREATE TABLE groups(identity TEXT PRIMARY KEY, group_key TEXT UNIQUE, data TEXT);"
        "CREATE TABLE value_sets(identity TEXT PRIMARY KEY, data TEXT);"
    )
    writer = Columnar(output / "raw.parquet", raw_schema())
    file = parquet.ParquetFile(path)
    count = selected = 0
    status_counts, dispositions = Counter(), Counter()
    source_digest = hashlib.sha256()
    try:
        for group in range(file.num_row_groups):
            table = file.read_row_group(group)
            if table.num_rows != source.row_groups[group]:
                raise ValueError("Independent DNS row-group count mismatch")
            for start in range(0, table.num_rows, 4096):
                check()
                for ordinal, raw in enumerate(table.slice(start, 4096).to_pylist(), start=start):
                    row = reference_row(raw, source, group, ordinal)
                    data = encoded(row)
                    database.execute(
                        "INSERT INTO raw VALUES (?,?)", (row["logical_record_id"], data)
                    )
                    writer.append(row)
                    source_digest.update(data.encode() + b"\n")
                    count += 1
                    status_counts[str(row["status_code"])] += 1
                    dispositions[row["disposition"]] += 1
                    if row["query_name"]:
                        database.execute(
                            "INSERT OR IGNORE INTO queries VALUES (?)",
                            (row["query_name"].lower().rstrip("."),),
                        )
                    if row["disposition"] == "selected_address":
                        key = encoded(
                            [
                                row["query_name"].lower().rstrip("."),
                                row["query_type"],
                                row["response_name"].lower().rstrip("."),
                                row["response_type"],
                            ]
                        )
                        database.execute(
                            "INSERT INTO addresses VALUES (?,?,?,?,?)",
                            (
                                key,
                                row["address_value"],
                                row["logical_record_id"],
                                row["worker_id"],
                                row["timestamp_ms"],
                            ),
                        )
                        selected += 1
                database.commit()
        writer.close()
        if count != sum(source.row_groups):
            raise ValueError("Independent DNS source coordinate gap")
        database.execute("CREATE INDEX address_keys ON addresses(group_key)")
        groups = Columnar(output / "groups.parquet", group_schema())
        links = Columnar(
            output / "contributors.parquet",
            arrow.schema([("group_id", arrow.string()), ("logical_record_id", arrow.string())]),
        )
        cursor = database.execute(
            "SELECT group_key,address,identity,worker,timestamp FROM addresses "
            "ORDER BY group_key,address,identity"
        )
        group_count = 0
        for key, values in itertools.groupby(cursor, key=lambda row: row[0]):
            members = list(itertools.islice(values, 4097))
            if len(members) > 4096:
                raise ValueError("DNS owned group evidence bound exceeded")
            addresses = sorted({row[1] for row in members})
            workers = sorted({row[3] for row in members if row[3] is not None})
            times = [row[4] for row in members if row[4] is not None]
            names = json.loads(key)
            identity = independent_id(
                "dns_group_observation",
                source.policy,
                (
                    source.manifest.source_id,
                    source.manifest.source_scope,
                    source.manifest.artifacts[0].sha256,
                    source.partition_date.isoformat(),
                    key,
                ),
            )
            value_id = hashlib.sha256(
                encoded(["dns-address-values-v1", addresses]).encode()
            ).hexdigest()
            record = dict(
                group_id=identity,
                group_key=key,
                artifact_sha256=source.manifest.artifacts[0].sha256,
                partition_date=source.partition_date.isoformat(),
                query_name=names[0],
                query_type=names[1],
                response_name=names[2],
                response_type=names[3],
                value_set_id=value_id,
                addresses_json=encoded(addresses),
                workers_json=encoded(workers),
                minimum_timestamp_ms=min(times) if times else None,
                maximum_timestamp_ms=max(times) if times else None,
                unknown_worker=int(any(row[3] is None for row in members)),
                unknown_time=int(len(times) != len(members)),
                contributors=len(members),
                entity_coverage="unknown",
                vantage="unverified",
            )
            groups.append(record)
            database.execute("INSERT INTO groups VALUES (?,?,?)", (identity, key, encoded(record)))
            database.execute(
                "INSERT OR IGNORE INTO value_sets VALUES (?,?)", (value_id, encoded(addresses))
            )
            for member in members:
                links.append(dict(group_id=identity, logical_record_id=member[2]))
            group_count += 1
        groups.close()
        links.close()
        database.commit()
        values_writer = Columnar(
            output / "values.parquet",
            arrow.schema([("value_set_id", arrow.string()), ("addresses_json", arrow.string())]),
        )
        for identity, data in database.execute(
            "SELECT identity,data FROM value_sets ORDER BY identity"
        ):
            values_writer.append(dict(value_set_id=identity, addresses_json=data))
        values_writer.close()
        raw_digest = hashlib.sha256()
        for (data,) in database.execute("SELECT data FROM raw ORDER BY identity"):
            raw_digest.update(data.encode() + b"\n")
        group_digest = hashlib.sha256()
        for (data,) in database.execute("SELECT data FROM groups ORDER BY identity"):
            group_digest.update(data.encode() + b"\n")
        summary = dict(
            source_rows=count,
            selected_address_rows=selected,
            address_groups=group_count,
            query_names=database.execute("SELECT COUNT(*) FROM queries").fetchone()[0],
            status_codes=dict(status_counts),
            dispositions=dict(dispositions),
            raw_projection_sha256=raw_digest.hexdigest(),
            source_order_sha256=source_digest.hexdigest(),
            group_projection_sha256=group_digest.hexdigest(),
            row_groups=list(source.row_groups),
            raw_coordinates_complete=True,
            entity_coverage="unknown",
            real_proven_empty_rows=0,
            elapsed_seconds=monotonic() - started,
        )
        (output / "reference.json").write_text(source.model_dump_json(indent=2))
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        return summary
    finally:
        database.close()


def comparison_ledger(previous, current, output):
    import pyarrow as arrow

    database = sqlite3.connect(":memory:")
    database.execute("ATTACH DATABASE ? AS previous", (str(previous / "oracle.sqlite"),))
    database.execute("ATTACH DATABASE ? AS current", (str(current / "oracle.sqlite"),))
    fields = [
        ("group_key", arrow.string()),
        ("previous_id", arrow.string()),
        ("current_id", arrow.string()),
        ("before", arrow.list_(arrow.string())),
        ("after", arrow.list_(arrow.string())),
        ("same_recorded_workers", arrow.bool_()),
        ("added_values", arrow.list_(arrow.string())),
        ("removed_values", arrow.list_(arrow.string())),
        ("unchanged_values", arrow.list_(arrow.string())),
        ("causal_interpretation", arrow.string()),
        ("comparison_policy", arrow.string()),
        ("strict_qualification", arrow.string()),
    ]
    writer = Columnar(output, arrow.schema(fields))
    checksum = hashlib.sha256()
    count = 0
    try:
        for old, new in database.execute(
            "SELECT p.data,c.data FROM previous.groups p JOIN current.groups c USING(group_key) "
            "ORDER BY p.group_key"
        ):
            old, new = json.loads(old), json.loads(new)
            before, after = json.loads(old["addresses_json"]), json.loads(new["addresses_json"])
            record = dict(
                group_key=old["group_key"],
                previous_id=old["group_id"],
                current_id=new["group_id"],
                before=before,
                after=after,
                same_recorded_workers=old["workers_json"] == new["workers_json"],
                added_values=sorted(set(after) - set(before)),
                removed_values=sorted(set(before) - set(after)),
                unchanged_values=sorted(set(before) & set(after)),
                causal_interpretation="unknown",
                comparison_policy="phase13-descriptive-v1",
                strict_qualification="vantage_unverified",
            )
            writer.append(record)
            checksum.update(encoded(record).encode() + b"\n")
            count += 1
        writer.close()
        return dict(rows=count, projection_sha256=checksum.hexdigest())
    finally:
        database.close()


def compare(previous, current):
    database = sqlite3.connect(":memory:")
    database.execute("ATTACH DATABASE ? AS previous", (str(previous / "oracle.sqlite"),))
    database.execute("ATTACH DATABASE ? AS current", (str(current / "oracle.sqlite"),))
    result = Counter()
    for old, new in database.execute(
        "SELECT p.data,c.data FROM previous.groups p JOIN current.groups c USING(group_key)"
    ):
        old, new = json.loads(old), json.loads(new)
        before, after = (
            set(json.loads(old["addresses_json"])),
            set(json.loads(new["addresses_json"])),
        )
        result["common_groups"] += 1
        result["changed_groups"] += before != after
        result["added_values"] += len(after - before)
        result["removed_values"] += len(before - after)
        result["unchanged_values"] += len(before & after)
        result["same_recorded_worker_sets"] += old["workers_json"] == new["workers_json"]
    result["new_observed_groups"] = database.execute(
        "SELECT COUNT(*) FROM current.groups c LEFT JOIN previous.groups p USING(group_key) "
        "WHERE p.identity IS NULL"
    ).fetchone()[0]
    result["no_longer_observed_groups"] = database.execute(
        "SELECT COUNT(*) FROM previous.groups p LEFT JOIN current.groups c USING(group_key) "
        "WHERE c.identity IS NULL"
    ).fetchone()[0]
    database.close()
    return dict(result) | dict(
        strict_matched_vantage_eligible=0,
        strict_unknown_qualification_groups=result["common_groups"],
        strict_reason="resolver/vantage and measurement-method version not independently supplied",
        causal_interpretation="unknown",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    parser.add_argument("--max-seconds", type=int, default=1200)
    args = parser.parse_args()
    if args.output.exists() or args.max_seconds != 1200:
        parser.error("Fresh oracle output and preregistered 1200-second cap required")
    started = monotonic()

    def check():
        from workloads.rpki_resources import directory_bytes

        if (
            monotonic() - started >= args.max_seconds
            or shutil.disk_usage(args.output.parent).free < 20 * 1024**3
            or directory_bytes(args.output) > 15 * 1024**3
        ):
            raise RuntimeError("DNS oracle resource/runtime guard exceeded")

    selected = json.loads(Path("workloads/results/dataset-feasibility/dns.json").read_text())[
        "days"
    ]
    summaries = []
    for day in selected:
        artifact = day["artifact"]
        path = args.cache / artifact["file"]
        source = reference(path, artifact)
        summaries.append(build(path, source, args.output / artifact["date"], check=check))
    (args.output / "comparison.json").write_text(
        json.dumps(
            compare(args.output / selected[0]["date"], args.output / selected[1]["date"]), indent=2
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
