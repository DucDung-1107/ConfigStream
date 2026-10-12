import argparse
import csv
import gzip
import hashlib
import io
import ipaddress
import json
import lzma
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

PARSER_VERSION = "feasibility-v1"
ALLOWED_HOSTS = {
    "ftp.ripe.net",
    "data.ris.ripe.net",
    "archive.routeviews.org",
    "zenodo.org",
    "raw.githubusercontent.com",
    "object.openintel.nl",
}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fetch(url, destination, *, limit=500_000_000, expected_sha256=None, headers=None):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError("Probe URL is not allowlisted HTTPS")
    if parsed.username or parsed.password:
        raise ValueError("Credential-bearing URL rejected")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    if shutil.disk_usage(destination.parent).free - limit < 20 * 1024**3:
        raise OSError("20 GiB disk reserve would be violated")
    if not destination.exists():
        temporary = destination.with_suffix(destination.suffix + ".part")
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "ConfigStream-research/1", **(headers or {})}
            )
            with urllib.request.urlopen(request, timeout=45) as response:
                if urllib.parse.urlsplit(response.url).hostname not in ALLOWED_HOSTS:
                    raise ValueError("Unapproved download redirect")
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > limit:
                    raise ValueError("Artifact exceeds probe budget")
                total = 0
                with temporary.open("wb") as output:
                    while chunk := response.read(1024 * 1024):
                        total += len(chunk)
                        if total > limit:
                            raise ValueError("Artifact exceeds probe budget")
                        if time.monotonic() - started > 300:
                            raise TimeoutError("Five-minute download deadline exceeded")
                        output.write(chunk)
                if declared and total != int(declared):
                    raise ValueError("Truncated artifact")
            temporary.replace(destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    if destination.stat().st_size > limit:
        raise ValueError("Cached artifact exceeds probe budget")
    checksum = digest(destination)
    if expected_sha256 and checksum != expected_sha256:
        raise ValueError("Artifact checksum mismatch")
    return {
        "url": url,
        "file": destination.name,
        "sha256": checksum,
        "artifact_bytes": destination.stat().st_size,
        "elapsed_seconds": time.monotonic() - started,
    }


def utc_timestamp(value, *, milliseconds=False):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / (1000 if milliseconds else 1), UTC)
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("Timestamp must carry an offset")
    return timestamp.astimezone(UTC)


def missing_dates(observed, start, days):
    first = date.fromisoformat(start)
    return [
        (first + timedelta(days=offset)).isoformat()
        for offset in range(days)
        if (first + timedelta(days=offset)).isoformat() not in observed
    ]


def state_diff(previous, current):
    return {
        "added": len(current - previous),
        "removed": len(previous - current),
        "unchanged": len(previous & current),
        "change_ratio_union": len(previous ^ current) / len(previous | current)
        if previous | current
        else 0,
    }


def parse_vrps(body, trust_anchor):
    reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")))
    required = {"ASN", "IP Prefix", "Max Length"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Unrecognized VRP schema")
    authorizations = set()
    rows = 0
    for row in reader:
        asn = int(row["ASN"].removeprefix("AS"))
        prefix = ipaddress.ip_network(row["IP Prefix"], strict=True)
        maximum = int(row["Max Length"])
        if not 0 <= asn <= 2**32 - 1 or not prefix.prefixlen <= maximum <= prefix.max_prefixlen:
            raise ValueError("Invalid authorization")
        authorizations.add((trust_anchor, asn, str(prefix), maximum))
        rows += 1
    return authorizations, {
        "rows": rows,
        "duplicates": rows - len(authorizations),
        "schema": reader.fieldnames,
    }


def rpki_probe(cache, output, *, start="2026-09-01", days=14, anchor="ripencc.tal"):
    if not 1 <= days <= 14 or anchor not in {"ripencc.tal", "apnic.tal", "arin.tal"}:
        raise ValueError("Probe is bounded to 14 days and known anchors")
    started = time.monotonic()
    records, artifacts, failures = [], [], []
    previous, previous_date = None, None
    distinct = set()
    logical_bytes = 0
    for offset in range(days):
        day = date.fromisoformat(start) + timedelta(days=offset)
        url = f"https://ftp.ripe.net/rpki/{anchor}/{day:%Y/%m/%d}/roas.csv.xz"
        try:
            path = cache / f"rpki-{anchor}-{day}.xz"
            checksum_path = cache / f"rpki-{anchor}-{day}.sha256"
            fetch(url + ".sha256", checksum_path, limit=4096)
            checksum = checksum_path.read_text().split()[0]
            artifact = fetch(url, path, expected_sha256=checksum)
            body = lzma.decompress(path.read_bytes())
            current, metadata = parse_vrps(body, anchor)
            distinct |= current
            logical_bytes += len(body)
            row = {
                "date": day.isoformat(),
                "unique_authorizations": len(current),
                "distinct_asns": len({value[1] for value in current}),
                "distinct_prefixes": len({value[2] for value in current}),
                "logical_csv_bytes": len(body),
                **metadata,
            }
            if previous is not None and (day - previous_date).days == 1:
                concentration = Counter(value[1] for value in current ^ previous)
                row.update(state_diff(previous, current))
                row["max_changes_one_asn"] = max(concentration.values(), default=0)
                row["changed_asns"] = len(concentration)
            records.append(row)
            artifacts.append(artifact)
            previous, previous_date = current, day
        except (OSError, ValueError, lzma.LZMAError) as error:
            failures.append({"date": day.isoformat(), "category": type(error).__name__})
    result = {
        "candidate": "B1",
        "parser": PARSER_VERSION,
        "scope": anchor,
        "start": start,
        "days_requested": days,
        "days": records,
        "artifacts": artifacts,
        "failures": failures,
        "missing_dates": missing_dates({record["date"] for record in records}, start, days),
        "logical_bytes": logical_bytes,
        "unique_entities_union": len(distinct),
        "elapsed_seconds": time.monotonic() - started,
        "change_unit": "authorization tuple symmetric difference; not operator commands",
    }
    write_json(output, result)
    return result


def mrt_events(record):
    message = record.get("bgp_message", {})
    if 2 not in message.get("type", {}):
        return []
    peer = (str(record["peer_as"]), record["peer_ip"])
    events = []
    for action, values in [
        ("withdrawal", message.get("withdrawn_routes", [])),
        ("announcement", message.get("nlri", [])),
    ]:
        for value in values:
            events.append((action, peer, value["prefix"], value["length"], value.get("path_id")))
    for attribute in message.get("path_attributes", []):
        attr_type = attribute.get("type", {})
        value = attribute.get("value", {})
        if 14 in attr_type or 15 in attr_type:
            if 1 not in value.get("safi", {}):
                raise ValueError("Non-unicast MP-NLRI requires a separate parser policy")
            action = "announcement" if 14 in attr_type else "withdrawal"
            for prefix in value.get("nlri", value.get("withdrawn_routes", [])):
                events.append(
                    (action, peer, prefix["prefix"], prefix["length"], prefix.get("path_id"))
                )
    return events


def bgp_probe(cache, output):
    import mrtparse

    started = time.monotonic()
    artifacts, failures = [], []
    actions, minutes, error_categories = Counter(), Counter(), Counter()
    prefixes, peers, fingerprints = set(), set(), set()
    records = updates = errors = duplicates = inversions = logical_bytes = 0
    first = last = previous_time = None
    for minute in range(0, 60, 5):
        name = f"updates.20260901.00{minute:02}.gz"
        path = cache / name
        try:
            artifacts.append(fetch(f"https://data.ris.ripe.net/rrc00/2026.09/{name}", path))
            with gzip.open(path, "rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    logical_bytes += len(chunk)
            for parsed in mrtparse.Reader(str(path)):
                records += 1
                if parsed.err:
                    errors += 1
                    error_categories[str(parsed.err_msg)] += 1
                    parsed.err = None
                    continue
                record = parsed.data
                events = mrt_events(record)
                if 2 in record.get("bgp_message", {}).get("type", {}):
                    updates += 1
                if not events:
                    continue
                timestamp = next(iter(record["timestamp"]))
                if previous_time is not None and timestamp < previous_time:
                    inversions += 1
                previous_time = timestamp
                first = timestamp if first is None else min(first, timestamp)
                last = timestamp if last is None else max(last, timestamp)
                attribute_hash = hashlib.sha256(
                    json.dumps(
                        record["bgp_message"].get("path_attributes", []), sort_keys=True
                    ).encode()
                ).digest()
                for action, peer, prefix, length, path_id in events:
                    actions[action] += 1
                    minutes[timestamp // 60] += 1
                    prefixes.add((prefix, length))
                    peers.add(peer)
                    identity = (timestamp, action, peer, prefix, length, path_id, attribute_hash)
                    fingerprint = hashlib.sha256(repr(identity).encode()).digest()
                    duplicates += fingerprint in fingerprints
                    fingerprints.add(fingerprint)
        except (OSError, ValueError) as error:
            failures.append({"file": name, "category": type(error).__name__})
    result = {
        "candidate": "D1",
        "parser": "mrtparse==2.2.0/" + PARSER_VERSION,
        "window": "2026-09-01T00:00:00Z/2026-09-01T01:00:00Z",
        "collector": "rrc00",
        "artifacts": artifacts,
        "failures": failures,
        "mrt_records": records,
        "bgp_update_messages": updates,
        "parse_errors": errors,
        "parse_error_categories": dict(error_categories),
        "prefix_events": sum(actions.values()),
        "actions": dict(actions),
        "unique_prefixes": len(prefixes),
        "unique_peers": len(peers),
        "duplicate_fingerprints": duplicates,
        "out_of_order_messages": inversions,
        "first_epoch": first,
        "last_epoch": last,
        "logical_mrt_bytes": logical_bytes,
        "events_per_second_fixed_hour": sum(actions.values()) / 3600,
        "peak_events_minute": max(minutes.values(), default=0),
        "events_per_minute": [[key * 60, value] for key, value in sorted(minutes.items())],
        "elapsed_seconds": time.monotonic() - started,
        "unit": "peer-specific unicast NLRI announcement/withdrawal; not config edits",
    }
    write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Bounded data-only feasibility probes")
    parser.add_argument("job", choices=["rpki", "bgp"])
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/phase13"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path("workloads/results/dataset-feasibility") / (args.job + ".json")
    result = (rpki_probe if args.job == "rpki" else bgp_probe)(args.cache_dir, output)
    print(json.dumps({"output": str(output), "failures": result["failures"]}))


if __name__ == "__main__":
    main()
