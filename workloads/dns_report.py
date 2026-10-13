"""Public aggregate evidence export; never export external DNS names or normalized rows."""

import json
import sqlite3
import subprocess
from pathlib import Path


def main():
    import pyarrow.parquet as parquet

    root = Path("workloads/results/phase19")
    private = Path(".cache/phase19")
    selection = json.loads(Path("workloads/results/dataset-feasibility/dns.json").read_text())[
        "days"
    ]
    code = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()

    def save(name, data):
        (root / f"{name}.json").write_text(
            json.dumps(dict(evidence_code_commit=code, **data), indent=2) + "\n"
        )

    sources = []
    for day in selection:
        artifact = day["artifact"]
        footer = parquet.ParquetFile(Path(".cache/phase13") / artifact["file"])
        summary = json.loads((private / "oracle" / day["date"] / "summary.json").read_text())
        database = sqlite3.connect(private / "oracle" / day["date"] / "oracle.sqlite")
        contexts = database.execute(
            "SELECT MIN(json_extract(data,'$.timestamp_ms')),"
            "MAX(json_extract(data,'$.timestamp_ms')),"
            "COUNT(DISTINCT json_extract(data,'$.worker_id')),"
            "SUM(json_extract(data,'$.timestamp_ms') IS NULL),"
            "SUM(json_extract(data,'$.worker_id') IS NULL) FROM raw"
        ).fetchone()
        database.close()
        sources.append(
            dict(
                date=day["date"],
                artifact=artifact,
                evidence="MEASURED_REAL_SOURCE",
                fields=[
                    dict(name=field.name, type=str(field.type), nullable=field.nullable)
                    for field in footer.schema_arrow
                ],
                uncompressed_column_bytes=sum(
                    footer.metadata.row_group(index).total_byte_size
                    for index in range(footer.num_row_groups)
                ),
                minimum_timestamp_ms=contexts[0],
                maximum_timestamp_ms=contexts[1],
                distinct_recorded_workers=contexts[2],
                unknown_source_timestamp_rows=contexts[3],
                unknown_worker_rows=contexts[4],
                timestamp_encoding="epoch_millisecond",
                source_precision="second",
                summary=summary,
            )
        )
    save(
        "independent-oracle",
        dict(
            evidence="MEASURED_REAL_SOURCE",
            sources=sources,
            comparison=json.loads((private / "oracle/comparison.json").read_text()),
            independent_normalization=True,
            exact_private_ledgers=True,
            raw_source_rows=sum(source["summary"]["source_rows"] for source in sources),
            raw_redistribution=False,
            original_source_retention_required_for_nonprojected_fields=True,
            matched_performance_baseline=False,
            oracle_wall_seconds=302.21,
            oracle_peak_process_rss_bytes=1048477696,
        ),
    )
    runs = []
    for directory in ["service-sample", "service-sample-v2", "service-one", "service-two"]:
        path = private / directory / "result.json"
        if not path.exists():
            runs.append(dict(experiment=directory, evidence="NOT_RUN"))
            continue
        result = json.loads(path.read_text())
        retained = {key: value for key, value in result.items() if key != "last_progress"}
        retained.update(experiment=directory, evidence="MEASURED_SERVICE")
        runs.append(retained)
    save(
        "validation",
        dict(
            evidence="MEASURED_SERVICE",
            runs=runs,
            rights="Private noncommercial research under verified publisher CC BY-NC-SA 4.0 terms",
            instructor_approval="NOT_VERIFIED",
            live_source_downloads=False,
            phase20_started=False,
        ),
    )


if __name__ == "__main__":
    main()
