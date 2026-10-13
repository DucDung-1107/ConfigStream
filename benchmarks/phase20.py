"""Opt-in bounded evaluation; no source downloads or automatic service replay."""

import argparse
import importlib.metadata
import json
import sqlite3
import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path
from time import monotonic

from benchmarks.evaluation import exact_encoding_digest, randomized_order, summary_digest
from benchmarks.rpki_service import GuestGuard
from workloads.rpki_resources import ResourceSampler, directory_bytes


class Cancellation:
    def __init__(self):
        self.cancelled = threading.Event()
        self.sparkContext = self

    def cancelAllJobs(self):
        self.cancelled.set()


@contextmanager
def guards(output, cap):
    session = Cancellation()
    resources = ResourceSampler(output, session)
    guest = GuestGuard(session, output)
    started = monotonic()
    guest.thread.start()

    def check():
        resources.check()
        guest.check()
        if session.cancelled.is_set() or monotonic() - started >= cap:
            raise RuntimeError("Evaluation resource/runtime guard failed")

    try:
        with resources:
            check()
            yield check
            check()
    finally:
        guest.stop.set()
        guest.thread.join(timeout=35)
        (output / "resources.json").write_text(
            json.dumps(
                dict(
                    host_samples=resources.samples,
                    guest_samples=guest.samples,
                    host_failure=resources.failure,
                    guest_failure=guest.failure,
                ),
                indent=2,
            )
            + "\n"
        )


def connection(path, check, *, readonly=False):
    database = (
        sqlite3.connect(f"file:{path}?mode=ro", uri=True) if readonly else sqlite3.connect(path)
    )
    database.execute("PRAGMA cache_size=-16384")
    database.execute("PRAGMA temp_store=FILE")
    database.set_progress_handler(lambda: int(check_progress(check)), 10000)
    return database


def check_progress(check):
    try:
        check()
        return False
    except RuntimeError:
        return True


def write_encodings(cursor, path, check):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    schema = arrow.schema([("member_encoding", arrow.string())])
    with parquet.ParquetWriter(path, schema, compression="snappy") as writer:
        while records := cursor.fetchmany(65536):
            check()
            values = [value.decode() if isinstance(value, bytes) else value for (value,) in records]
            writer.write_table(
                arrow.Table.from_arrays([arrow.array(values)], schema=schema), row_group_size=65536
            )


def encodings(path, check):
    import pyarrow.parquet as parquet

    for batch in parquet.ParquetFile(path).iter_batches(batch_size=8192):
        check()
        yield from ((value.encode(),) for value in batch.column(0).to_pylist())


def oracle_rows(oracle, day):
    return oracle.execute("SELECT encoding FROM members WHERE day=? ORDER BY encoding", (day,))


def delta_rows(oracle, current, previous, action):
    left, right = (current, previous) if action == "added" else (previous, current)
    return oracle.execute(
        "SELECT encoding FROM members WHERE day=? EXCEPT "
        "SELECT encoding FROM members WHERE day=? ORDER BY encoding",
        (left, right),
    )


def build_rpki(mode, oracle, days, output, check):
    baseline_indices = range(len(days)) if mode == "R1" else [0] if mode == "R2" else [0, 7]
    for index in baseline_indices:
        write_encodings(oracle_rows(oracle, days[index]), output / f"full-{index}.parquet", check)
    for index in range(1, len(days)):
        for action in ("added", "removed"):
            write_encodings(
                delta_rows(oracle, days[index], days[index - 1], action),
                output / f"{action}-{index}.parquet",
                check,
            )


def reconstruct(database, mode, index, output, check):
    baseline = index if mode == "R1" else 0 if mode == "R2" or index < 7 else 7
    database.execute("DELETE FROM state")
    database.executemany(
        "INSERT INTO state VALUES (?)", encodings(output / f"full-{baseline}.parquet", check)
    )
    for boundary in range(baseline + 1, index + 1):
        for (encoding,) in encodings(output / f"removed-{boundary}.parquet", check):
            if database.execute("DELETE FROM state WHERE encoding=?", (encoding,)).rowcount != 1:
                raise ValueError("Delta removes nonexistent canonical membership")
        database.executemany(
            "INSERT INTO state VALUES (?)", encodings(output / f"added-{boundary}.parquet", check)
        )
    database.commit()


def rpki_queries(mode, oracle, days, output, cycle, check):
    database = connection(output / f"scratch-{cycle}.sqlite", check)
    database.execute("CREATE TABLE state(encoding BLOB PRIMARY KEY) WITHOUT ROWID")
    query_results = []

    def run(query_id, query):
        began = monotonic()
        value = query()
        query_results.append(
            dict(
                query_id=query_id,
                cycle=cycle,
                seconds=monotonic() - began,
                correctness="PASS",
                output_sha256=summary_digest(value),
            )
        )

    def state(index):
        reconstruct(database, mode, index, output, check)
        return exact_encoding_digest(
            database.execute("SELECT encoding FROM state ORDER BY encoding"),
            oracle_rows(oracle, days[index]),
            check,
        )

    run("all_days", lambda: [state(index) for index in range(len(days))])
    run("selected_day", lambda: state(len(days) - 1))

    def membership():
        reconstruct(database, mode, len(days) - 1, output, check)
        target = oracle_rows(oracle, days[0]).fetchone()[0]
        actual = database.execute(
            "SELECT count(*) FROM state WHERE encoding=?", (target,)
        ).fetchone()[0]
        expected = oracle.execute(
            "SELECT count(*) FROM members WHERE day=? AND encoding=?", (days[-1], target)
        ).fetchone()[0]
        if actual != expected:
            raise ValueError("Membership query mismatch")
        return actual

    run("membership", membership)

    def transitions(by_asn=False):
        result = {}
        for index in range(1, len(days)):
            for action in ("added", "removed"):
                observed = encodings(output / f"{action}-{index}.parquet", check)
                expected = delta_rows(oracle, days[index], days[index - 1], action)
                if by_asn:
                    from itertools import zip_longest

                    for actual, reference in zip_longest(observed, expected):
                        if actual is None or reference is None or actual != reference:
                            raise ValueError("Transition tuple mismatch")
                        asn = json.loads(actual[0])[1]
                        key = f"{asn}:{action}"
                        result[key] = result.get(key, 0) + 1
                else:
                    result[f"{index}:{action}"] = exact_encoding_digest(observed, expected, check)
        return result

    run("daily_transitions", transitions)
    run("asn_transitions", lambda: transitions(True))
    database.close()
    return query_results


def rpki(mode, output, check):
    summary = json.loads(Path(".cache/phase17/oracle/summary.json").read_text())
    days = [item["date"] for item in summary["days"]]
    if len(days) != 14:
        raise ValueError("Frozen fourteen-day oracle required")
    oracle = connection(Path(".cache/phase17/oracle/oracle.sqlite"), check, readonly=True)
    for day in summary["days"]:
        verified = exact_encoding_digest(
            oracle_rows(oracle, day["date"]), oracle_rows(oracle, day["date"]), check
        )
        if verified != dict(rows=day["unique_members"], set_digest=day["set_digest"]):
            raise ValueError("Independent oracle changed since accepted source verification")
    started = monotonic()
    build_rpki(mode, oracle, days, output, check)
    built = monotonic() - started
    queries = [
        item
        for cycle in range(1, 4)
        for item in rpki_queries(mode, oracle, days, output, cycle, check)
    ]
    oracle.close()
    return dict(
        mode=mode,
        build_seconds=built,
        queries=queries,
        analytical_bytes=sum(path.stat().st_size for path in output.glob("*.parquet")),
        parquet_files=len(list(output.glob("*.parquet"))),
        scratch_retained_bytes=sum(path.stat().st_size for path in output.glob("scratch*")),
        expected_set_digests=[day["set_digest"] for day in summary["days"]],
        reconstruction="exact sorted tuple comparison for every day, every cycle",
    )


def pilot(mode, output, check):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    selections = {
        "rpki": (Path(".cache/phase17/oracle/members/2026-09-01.parquet"), None),
        "ris": (
            Path(".cache/phase18-closure/oracle-hour-v2/updates.20260901.0000.gz/events.parquet"),
            None,
        ),
        "dns": (Path(".cache/phase19/oracle/2026-09-01/raw.parquet"), 4096),
    }
    source, limit = selections[mode]
    original = parquet.ParquetFile(source)
    count = 0
    began = monotonic()
    with parquet.ParquetWriter(
        output / "normalized.parquet", original.schema_arrow, compression="snappy"
    ) as writer:
        for batch in original.iter_batches(batch_size=65536):
            check()
            if limit is not None:
                batch = batch.slice(0, min(batch.num_rows, limit - count))
            count += batch.num_rows
            writer.write_table(arrow.Table.from_batches([batch]), row_group_size=65536)
            if limit is not None and count >= limit:
                break
    write_seconds = monotonic() - began
    began = monotonic()
    rewritten = parquet.ParquetFile(output / "normalized.parquet")
    observed = rewritten.iter_batches(batch_size=8192)
    compared = 0
    for expected in original.iter_batches(batch_size=8192):
        check()
        if limit is not None:
            expected = expected.slice(0, min(expected.num_rows, limit - compared))
        actual = next(observed, None)
        if actual is None or not actual.equals(expected):
            raise ValueError("Pilot exact full projection mismatch")
        compared += actual.num_rows
        if limit is not None and compared >= limit:
            break
    if next(observed, None) is not None or compared != count:
        raise ValueError("Unexpected/missing pilot rows")
    return dict(
        mode=mode,
        rows=count,
        write_seconds=write_seconds,
        validation_seconds=monotonic() - began,
        source_normalized_bytes=source.stat().st_size,
        output_bytes=(output / "normalized.parquet").stat().st_size,
        exact_projection="PASS",
        parsing="EXCLUDED: accepted normalized oracle input",
        complete_source_scope=limit is None,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=["pilot", "rpki"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--worker")
    parser.add_argument("--repetition", type=int)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh private output required")
    args.output.mkdir(parents=True)
    cap = 600 if args.suite == "pilot" else 900
    if not args.worker:
        configurations = ["rpki", "ris", "dns"] if args.suite == "pilot" else ["R1", "R2", "R3"]
        order = randomized_order(configurations)
        (args.output / "order.json").write_text(json.dumps(order, indent=2))
        for mode, repetition in order:
            target = args.output / f"{mode}-{repetition}"
            command = [
                str(Path(".venv/bin/python").resolve()),
                "-m",
                "benchmarks.phase20",
                "--suite",
                args.suite,
                "--output",
                str(target),
                "--worker",
                mode,
                "--repetition",
                str(repetition),
            ]
            with (args.output / f"{mode}-{repetition}.log").open("w") as log:
                subprocess.run(
                    command, stdout=log, stderr=subprocess.STDOUT, timeout=cap + 45, check=True
                )
        return
    result = dict(
        status="FAIL",
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        suite=args.suite,
        mode=args.worker,
        repetition=args.repetition,
        evidence="MEASURED_REAL_SOURCE_NORMALIZED",
        runtime_cap_seconds=cap,
        engine="PyArrow19.0.1/SQLite" if args.suite == "rpki" else "PyArrow19.0.1",
        sqlite_version=sqlite3.sqlite_version,
        pyarrow_version=importlib.metadata.version("pyarrow"),
        timing="worker start through build, exact comparison and query cycles; parser excluded",
        cache="fresh process, OS cache uncontrolled; query cycles reused process",
        source_manifests="workloads/results/phase20/experiment-registration.json",
        resources="resources.json",
        argv_mode="isolated private local-only output",
    )
    began = monotonic()
    try:
        with guards(args.output, cap) as check:
            result.update(
                (pilot if args.suite == "pilot" else rpki)(args.worker, args.output, check)
            )
        result["status"] = "PASS"
    except Exception as error:
        result["error_category"] = type(error).__name__
        raise
    finally:
        result["elapsed_seconds"] = monotonic() - began
        result["private_output_bytes"] = directory_bytes(args.output)
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
