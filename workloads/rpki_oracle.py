"""Independent disk-backed reference: no Spark or production RPKI transform imports."""

import csv
import ipaddress
import json
import lzma
import sqlite3
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from time import monotonic


def encoded_tuple(scope, asn, prefix, length):
    return json.dumps(
        [scope, asn, prefix, length], separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def independent_member_id(scope, asn, prefix, length):
    preimage = [
        "configstream-temporal-v1",
        "rpki_membership",
        "membership-v1",
        [["string", scope], ["integer", asn], ["string", prefix], ["integer", length]],
    ]
    return sha256(
        json.dumps(preimage, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def set_checksum(cursor):
    digest = sha256(b"rpki-set-v1\n")
    for (encoded,) in cursor:
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def export_members(database, day, path: Path):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    schema = arrow.schema(
        [
            ("member_encoding", arrow.string()),
            ("membership_id", arrow.string()),
            ("scope", arrow.string()),
            ("asn", arrow.int64()),
            ("prefix", arrow.string()),
            ("max_length", arrow.int32()),
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with parquet.ParquetWriter(path, schema, compression="snappy") as writer:
        cursor = database.execute(
            "SELECT encoding FROM members WHERE day=? ORDER BY encoding", (day,)
        )
        while records := cursor.fetchmany(4096):
            rows = []
            for (encoding,) in records:
                scope, asn, prefix, length = json.loads(encoding)
                rows.append(
                    dict(
                        member_encoding=encoding.decode(),
                        membership_id=independent_member_id(scope, asn, prefix, length),
                        scope=scope,
                        asn=asn,
                        prefix=prefix,
                        max_length=length,
                    )
                )
            writer.write_table(arrow.Table.from_pylist(rows, schema=schema))


def export_source_map(source_path, scope, path):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    schema = arrow.schema([("row_index", arrow.int64()), ("member_encoding", arrow.string())])
    checksum = sha256(b"rpki-source-map-v1\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with parquet.ParquetWriter(path, schema, compression="snappy") as writer:
        with lzma.open(source_path, "rt", encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            if next(reader) != ["URI", "ASN", "IP Prefix", "Max Length", "Not Before", "Not After"]:
                raise ValueError("Oracle raw-map schema mismatch")
            rows = []
            for index, fields in enumerate(reader, 1):
                count = index
                if len(fields) != 6 or not fields[1].startswith("AS"):
                    raise ValueError("Oracle invalid raw-map source row")
                asn, network, length = (
                    int(fields[1][2:]),
                    ipaddress.ip_network(fields[2], strict=True),
                    int(fields[3]),
                )
                if (
                    not 0 <= asn <= 4294967295
                    or not network.prefixlen <= length <= network.max_prefixlen
                ):
                    raise ValueError("Oracle invalid raw-map member")
                encoding = encoded_tuple(scope, asn, str(network), length).decode()
                projected = json.dumps(
                    [index, encoding], ensure_ascii=False, separators=(",", ":")
                ).encode()
                checksum.update(len(projected).to_bytes(4, "big"))
                checksum.update(projected)
                rows.append(dict(row_index=index, member_encoding=encoding))
                if len(rows) == 4096:
                    writer.write_table(arrow.Table.from_pylist(rows, schema=schema))
                    rows.clear()
            if rows:
                writer.write_table(arrow.Table.from_pylist(rows, schema=schema))
    return dict(source_rows=count, source_map_digest=checksum.hexdigest())


def run_oracle(selection: dict, cache: Path, output: Path, *, columnar: bool = True) -> dict:
    if not 1 <= len(selection["artifacts"]) <= 14:
        raise ValueError("Independent oracle window budget exceeded")
    output.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(output / "oracle.sqlite")
    database.execute("PRAGMA cache_size=-16384")
    database.execute("PRAGMA temp_store=FILE")
    database.execute(
        "CREATE TABLE IF NOT EXISTS members(day TEXT, encoding BLOB, "
        "PRIMARY KEY(day,encoding)) WITHOUT ROWID"
    )
    database.execute("CREATE TABLE IF NOT EXISTS complete(day TEXT PRIMARY KEY, summary TEXT)")
    daily = []
    started = monotonic()
    try:
        for item in selection["artifacts"]:
            if Path(item["file"]).name != item["file"]:
                raise ValueError("Unsafe oracle source filename")
            day = item["file"].removeprefix(f"rpki-{selection['scope']}-").removesuffix(".xz")
            date.fromisoformat(day)
            path = cache / item["file"]
            if (
                path.stat().st_size != item["artifact_bytes"]
                or item["artifact_bytes"] > 64 * 1024**2
            ):
                raise ValueError("Oracle artifact checksum/size budget mismatch")
            digest = sha256()
            size = 0
            with path.open("rb") as source:
                while chunk := source.read(65536):
                    size += len(chunk)
                    digest.update(chunk)
            if digest.hexdigest() != item["sha256"] or size != item["artifact_bytes"]:
                raise ValueError(f"Oracle artifact checksum mismatch: {day}")
            saved = database.execute("SELECT summary FROM complete WHERE day=?", (day,)).fetchone()
            if saved:
                summary = json.loads(saved[0])
                if summary["artifact_sha256"] != item["sha256"]:
                    raise ValueError("Conflicting same-day source revision")
                daily.append(summary)
                if columnar:
                    export_members(database, day, output / "members" / f"{day}.parquet")
                    raw = export_source_map(
                        path, selection["scope"], output / "source_maps" / f"{day}.parquet"
                    )
                    if raw["source_rows"] != summary["source_rows"]:
                        raise ValueError("Oracle source map accounting mismatch")
                    summary.update(raw)
                continue
            tick = monotonic()
            rows, logical = 0, 0
            with database:
                database.execute("DELETE FROM members WHERE day=?", (day,))
                with lzma.open(path, "rb") as stream:
                    header = stream.readline(65537)
                    logical += len(header)
                    if (
                        header.decode().strip()
                        != "URI,ASN,IP Prefix,Max Length,Not Before,Not After"
                    ):
                        raise ValueError("Oracle unexpected CSV schema")
                    pending = []
                    for line in iter(lambda: stream.readline(65537), b""):
                        rows += 1
                        logical += len(line)
                        if len(line) > 65536 or logical > 256 * 1024**2:
                            raise ValueError("Oracle decompression budget exceeded")
                        values = next(csv.reader([line.decode("utf-8")], strict=True))
                        if len(values) != 6 or not values[1].startswith("AS"):
                            raise ValueError(f"Oracle invalid row coordinate {day}/{rows}")
                        if not values[1][2:].isascii() or not values[1][2:].isdigit():
                            raise ValueError("Oracle invalid ASN encoding")
                        asn = int(values[1][2:])
                        try:
                            network = ipaddress.ip_network(values[2], strict=True)
                        except ValueError:
                            raise ValueError(f"Oracle invalid prefix at {day}/{rows}") from None
                        if not values[3].isascii() or not values[3].isdigit():
                            raise ValueError("Oracle invalid maxLength encoding")
                        length = int(values[3])
                        if (
                            not 0 <= asn <= 4294967295
                            or not network.prefixlen <= length <= network.max_prefixlen
                        ):
                            raise ValueError(f"Oracle invalid membership at {day}/{rows}")
                        pending.append(
                            (day, encoded_tuple(selection["scope"], asn, str(network), length))
                        )
                        if len(pending) == 4096:
                            database.executemany(
                                "INSERT OR IGNORE INTO members VALUES (?,?)", pending
                            )
                            pending.clear()
                    database.executemany("INSERT OR IGNORE INTO members VALUES (?,?)", pending)
                unique = database.execute(
                    "SELECT COUNT(*) FROM members WHERE day=?", (day,)
                ).fetchone()[0]
                checksum = set_checksum(
                    database.execute(
                        "SELECT encoding FROM members WHERE day=? ORDER BY encoding", (day,)
                    )
                )
                summary = dict(
                    date=day,
                    scope=selection["scope"],
                    artifact_sha256=item["sha256"],
                    compressed_bytes=size,
                    logical_csv_bytes=logical,
                    source_rows=rows,
                    accepted_rows=rows,
                    excluded_rows=0,
                    unique_members=unique,
                    duplicate_rows=rows - unique,
                    set_digest=checksum,
                    parse_seconds=monotonic() - tick,
                )
                database.execute("INSERT INTO complete VALUES (?,?)", (day, json.dumps(summary)))
            if columnar:
                export_members(database, day, output / "members" / f"{day}.parquet")
                summary.update(
                    export_source_map(
                        path, selection["scope"], output / "source_maps" / f"{day}.parquet"
                    )
                )
            daily.append(summary)
            print(f"oracle completed {day}: {rows} rows", flush=True)
        boundaries = []
        database.execute("DROP TABLE IF EXISTS reconstructed")
        database.execute("CREATE TABLE reconstructed(encoding BLOB PRIMARY KEY) WITHOUT ROWID")
        database.execute(
            "INSERT INTO reconstructed SELECT encoding FROM members WHERE day=?",
            (daily[0]["date"],),
        )
        reconstruction = []
        for index, current in enumerate(daily):
            day = current["date"]
            if index:
                old = daily[index - 1]["date"]
                if date.fromisoformat(day) - date.fromisoformat(old) != timedelta(days=1):
                    raise ValueError("Oracle cannot bridge missing day")
                difference = (
                    "SELECT encoding FROM members WHERE day=? EXCEPT "
                    "SELECT encoding FROM members WHERE day=?"
                )
                addition_count = database.execute(
                    "SELECT COUNT(*) FROM (" + difference + ")", (day, old)
                ).fetchone()[0]
                removal_count = database.execute(
                    "SELECT COUNT(*) FROM (" + difference + ")", (old, day)
                ).fetchone()[0]
                transition_digest = sha256(b"rpki-transition-members-v1\n")
                for action, first, second in (("add", day, old), ("remove", old, day)):
                    for (encoding,) in database.execute(
                        difference + " ORDER BY encoding", (first, second)
                    ):
                        payload = action.encode() + b":" + encoding
                        transition_digest.update(len(payload).to_bytes(4, "big"))
                        transition_digest.update(payload)
                database.execute(
                    "DELETE FROM reconstructed WHERE encoding IN (" + difference + ")", (old, day)
                )
                database.execute("INSERT INTO reconstructed " + difference, (day, old))
                boundaries.append(
                    dict(
                        previous_date=old,
                        current_date=day,
                        additions=addition_count,
                        removals=removal_count,
                        transition_member_digest=transition_digest.hexdigest(),
                    )
                )
            missing = database.execute(
                "SELECT COUNT(*) FROM (SELECT encoding FROM members WHERE day=? "
                "EXCEPT SELECT encoding FROM reconstructed)",
                (day,),
            ).fetchone()[0]
            extra = database.execute(
                "SELECT COUNT(*) FROM (SELECT encoding FROM reconstructed "
                "EXCEPT SELECT encoding FROM members WHERE day=?)",
                (day,),
            ).fetchone()[0]
            reconstructed = set_checksum(
                database.execute("SELECT encoding FROM reconstructed ORDER BY encoding")
            )
            if missing or extra or reconstructed != current["set_digest"]:
                raise ValueError("Oracle reconstruction failure")
            reconstruction.append(
                dict(date=day, missing=missing, unexpected=extra, digest=reconstructed)
            )
        database.commit()
        union = database.execute("SELECT COUNT(DISTINCT encoding) FROM members").fetchone()[0]
        result = dict(
            oracle_version="sqlite-independent-v1",
            days=daily,
            boundaries=boundaries,
            reconstruction=reconstruction,
            unique_members_window=union,
            elapsed_seconds=monotonic() - started,
        )
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        return result
    finally:
        database.close()
