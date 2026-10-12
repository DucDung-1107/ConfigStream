import argparse
import ipaddress
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

from workloads.feasibility import digest, state_diff, write_json


def address_observation(row):
    kind = row.get("response_type")
    field = {"A": "ip4_address", "AAAA": "ip6_address"}.get(kind)
    if field is None or row.get("status_code") != 0 or not row.get(field):
        return None
    key = (
        row["query_name"].lower().rstrip("."),
        row["query_type"],
        row["response_name"].lower().rstrip("."),
        kind,
    )
    return key, str(ipaddress.ip_address(row[field]))


def dns_probe(cache, output):
    import pyarrow.parquet as parquet

    started = time.monotonic()
    artifacts = json.loads((cache / "dns-artifacts.json").read_text())
    days, failures = [], []
    previous = None
    for artifact in artifacts:
        if "failure" in artifact:
            failures.append(artifact)
            continue
        path = cache / artifact["file"]
        if path.stat().st_size != artifact["artifact_bytes"] or digest(path) != artifact["sha256"]:
            raise ValueError("DNS artifact checksum/size mismatch")
        file = parquet.ParquetFile(path)
        counts, queries, states = Counter(), set(), defaultdict(set)
        rows = address_rows = duplicate_rows = arrow_bytes = canonical_bytes = 0
        first = last = None
        columns = [
            "query_name",
            "query_type",
            "response_name",
            "response_type",
            "timestamp",
            "status_code",
            "ip4_address",
            "ip6_address",
        ]
        for batch in file.iter_batches(batch_size=32768, columns=columns):
            arrow_bytes += batch.nbytes
            for row in batch.to_pylist():
                rows += 1
                counts[row["response_type"] or "NO_RESPONSE"] += 1
                queries.add(row["query_name"].lower().rstrip("."))
                timestamp = row["timestamp"]
                first = timestamp if first is None else min(first, timestamp)
                last = timestamp if last is None else max(last, timestamp)
                observation = address_observation(row)
                if observation:
                    address_rows += 1
                    key, value = observation
                    duplicate_rows += value in states[key]
                    states[key].add(value)
        for key, values in states.items():
            canonical_bytes += len(
                json.dumps([key, sorted(values)], separators=(",", ":")).encode()
            )
        day = {
            "date": artifact["date"],
            "artifact": artifact,
            "rows": rows,
            "query_names": len(queries),
            "record_types": dict(counts),
            "address_rows": address_rows,
            "duplicate_address_values": duplicate_rows,
            "address_groups": len(states),
            "canonical_address_json_bytes": canonical_bytes,
            "selected_arrow_bytes": arrow_bytes,
            "parquet_uncompressed_column_bytes": sum(
                file.metadata.row_group(group).column(column).total_uncompressed_size
                for group in range(file.metadata.num_row_groups)
                for column in range(file.metadata.num_columns)
            ),
            "first_timestamp_ms": first,
            "last_timestamp_ms": last,
            "schema": str(file.schema_arrow),
        }
        if previous is not None:
            common = previous.keys() & states.keys()
            changes = Counter()
            for key in common:
                difference = state_diff(previous[key], states[key])
                changes["added_values"] += difference["added"]
                changes["removed_values"] += difference["removed"]
                changes["unchanged_values"] += difference["unchanged"]
                changes["changed_groups"] += bool(difference["added"] or difference["removed"])
            day["comparison"] = {
                **dict(changes),
                "common_groups": len(common),
                "new_observed_groups": len(states.keys() - previous.keys()),
                "no_longer_observed_groups": len(previous.keys() - states.keys()),
            }
        days.append(day)
        previous = states
    result = {
        "candidate": "C1",
        "parser": "pyarrow==19.0.1/feasibility-dns-v1",
        "scope": ".li / whole published daily partitions / A+AAAA comparisons only",
        "days": days,
        "failures": failures,
        "elapsed_seconds": time.monotonic() - started,
        "limitation": "Observed address-set differences may reflect measurement or DNS balancing; "
        "not confirmed zone edits. TTL, signatures, geo fields, TXT and SOA email excluded.",
    }
    write_json(output, result)
    return result


def zone_probe(output, seconds=30):
    import websocket

    if not 1 <= seconds <= 30:
        raise ValueError("Live probe must end within 30 seconds")
    started = time.monotonic()
    start_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    count, timestamps, payload_bytes = 0, 0, 0
    keys, failure, connection = set(), None, None
    try:
        connection = websocket.create_connection(
            "wss://zonestream.openintel.nl/ws/zone_diff", timeout=8
        )
        while time.monotonic() - started < seconds and count < 10000:
            connection.settimeout(min(3, max(0.1, seconds - (time.monotonic() - started))))
            try:
                body = connection.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not body:
                break
            record = json.loads(body)
            count += 1
            payload_bytes += len(body.encode() if isinstance(body, str) else body)
            if isinstance(record, dict):
                keys.update(record)
                timestamps += record.get("timestamp") is not None
    except (OSError, ValueError, websocket.WebSocketException) as error:
        failure = {
            "category": type(error).__name__,
            "http_status": getattr(error, "status_code", None),
        }
    finally:
        if connection is not None:
            connection.close()
    result = {
        "candidate": "C2",
        "url": "wss://zonestream.openintel.nl/ws/zone_diff",
        "started_utc": start_utc,
        "bounded_seconds": seconds,
        "observed_messages": count,
        "elapsed_seconds": time.monotonic() - started,
        "payload_bytes": payload_bytes,
        "schema_keys": sorted(keys),
        "source_timestamps_present": timestamps,
        "failure": failure,
        "historical_archive_verified": False,
        "parser": "websocket-client==1.9.0/feasibility-zone-v1",
        "note": "No raw domain payload retained; zero messages is not zero population rate.",
    }
    write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=["dns", "zonestream"], default="dns")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/phase13"))
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--accept-openintel-terms", action="store_true")
    parser.add_argument(
        "--output", type=Path, default=Path("workloads/results/dataset-feasibility/dns.json")
    )
    args = parser.parse_args()
    if args.download:
        if args.job != "dns" or not args.accept_openintel_terms:
            parser.error("DNS download requires explicit acceptance of OpenINTEL terms")
        from workloads.feasibility import fetch

        manifest = json.loads(
            Path("workloads/results/dataset-feasibility/probe-manifest.json").read_text()
        )
        artifacts = []
        for artifact in manifest["artifacts"]:
            if artifact["url"].startswith("https://object.openintel.nl/"):
                artifacts.append(
                    fetch(
                        artifact["url"],
                        args.cache_dir / artifact["file"],
                        expected_sha256=artifact["sha256"],
                        headers={"Cookie": "openintel-data-agreement-accepted=true"},
                    )
                )
        if len(artifacts) != 2:
            parser.error("Expected exactly two pinned DNS artifacts")
        write_json(args.cache_dir / "dns-artifacts.json", artifacts)
    result = (
        dns_probe(args.cache_dir, args.output) if args.job == "dns" else zone_probe(args.output)
    )
    print(json.dumps({"output": str(args.output), "candidate": result["candidate"]}))


if __name__ == "__main__":
    main()
