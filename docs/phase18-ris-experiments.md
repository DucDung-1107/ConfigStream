# Phase 18 — staged experiments and measured limits

## Selection and actual stages

Source is RIPE NCC RIS `rrc00`, September 1, 2026, twelve five-minute archives
00:00–00:55 UTC, pinned by Phase 13 SHA-256/size. All are already cached; no
new source downloads. [Source manifest](../workloads/results/phase18/source-manifest.json)
contains the exact URLs/hashes. Full framing independently accounts for all
2,300,578 records and 427,068,595 decompressed bytes, including 57 body exclusions.
This is **not** a full-hour event-pipeline experiment.

| Stage | Actual evidence | Status |
|---|---|---|
| A owned bytes | Conventional/MP/ADD-PATH, framing errors, identity, local storage, real broker/checkpoint/S3 fault fixtures | PASS scoped tests |
| B complete first archive | 370,141 occurrences through actual Kafka–Spark–MinIO; exact selected oracle projection, physical readback, queries | PASS selected window; oracle attributes qualified |
| C first fifteen minutes | Code accepts three pinned artifacts; no service run | NOT_RUN, independent full-projection gate remains open |
| D full hour | Complete hashes/framing/exclusions only; 4,725,156 occurrences and 118,172 repeats remain old survey references | NOT_RUN occurrence/service acceptance |

The independent binary oracle verifies native identity, offsets, ordinal, peer,
source time, action, AFI/prefix and path ID. Attributes/fingerprints still depend
on pinned mrtparse; see [correctness](phase18-ris-identity-and-correctness.md).
Larger service runs are gated on oracle strengthening, not merely count inflation.

## Accepted first-archive measurements

Code commit `314897f4ec79d34a93780096b6294fb9b19958cc`; isolated query
`90d00db4-e0c2-4a53-937f-2d067e9dbaa7`. Window is 00:00–00:05 UTC.

| Measurement | Observed | Definition |
|---|---:|---|
| Compressed / decompressed input bytes | 6,043,608 / 34,706,880 | Same pinned gzip, full EOF/CRC |
| Framed / excluded records | 187,480 / 5 | Source positions, not event occurrences |
| Parsed UPDATE messages | 186,717 | Accepted parser UPDATEs, including any zero-prefix control |
| Logical occurrences | 370,141 | 349,085 announcements + 21,056 withdrawals |
| Peers / prefixes | 98 / 64,607 | Selected observation population only |
| Repeats beyond first fingerprint | 11,351 | Preserved; 358,790 unique fingerprint keys |
| Source-time average / peak minute | 1,233.8033/s / 131,108 | Fixed 300-second archive window; not pipeline throughput |
| Kafka acknowledged value bytes / chunks | 269,418,671 / 2,892 | JSON values, not wire traffic |
| Producer seconds | 153.798 | Parsing, validation, ACK/progress included |
| Callback sum seconds | 104.733 | Twelve callbacks; includes reconciliation, digests, publication |
| Whole-run / final validation seconds | 283.846 / 16.803 | Includes Spark startup, sequential publishing/drain and readback |
| Parquet / all selected S3-prefix bytes | 70,024,542 / 70,180,672 | 48 Parquet files, twelve commits |
| Checkpoint bytes | 10,947 | Selected checkpoint tree only |
| Final lag | 0 on both partitions | Actual broker high watermarks versus final Spark offsets |
| Peak sampled Spark process-tree RSS | 2,158,067,712 bytes | Sampling, not instantaneous physical peak |
| Minimum guest free disk / available RAM | 44,806,324,224 / 4,517,789,696 bytes | 28 guest samples; 136 host samples |

The pipeline is unpaced historical ingestion: publishing completes before
`availableNow` drain. It does not establish steady live backpressure, a natural-rate
SLA, CPU utilization, end-to-end per-event latency, horizontal scalability, or
Spark superiority. Source-time histogram and processing denominators are separate
JSON artifacts. Spark progress input rows count Kafka chunks and can reflect
repeated Spark scans; native occurrence counts come from the reconciled ledger.

## Negative attempt and bounds

The earlier isolated query `5820f967-5434-431f-8d5d-125372df90b6` reached committed
history but failed final report serialization because progress contained UUIDs.
It remains **FAIL_REPORTING**, not accepted performance evidence. Its files,
checkpoint, topic and logs were preserved. A bounded new run after the reporting
fix used the same input and 1,800-second cap; no timeout increase occurred.
The minor metadata correction is [P18-M1](phase18-contract-amendment.md).

Registered guards: 20 GiB free host/guest disk, 15 GiB per-run growth, 50 GiB new
host experiment cap, 12 GiB sampled Spark-tree RSS, at least 512 MiB available
guest RAM, no concurrent heavy benchmark, and an independent RIS 1,800-second
run cap. All accepted-run guards passed. The RPKI 120-minute approval was not reused.
Full-hour runtime/space estimates are **unknown beyond this pilot**; no broader
runtime authorization or experimental acceptance is inferred.

## Batch and future comparisons

The source oracle also writes a basic Arrow/Parquet projection: 65,722,314 bytes,
with exact supported-subset comparison, SQL fingerprint inventory and source-order
digest. Its 90.525-second execution includes two framing/parser passes and oracle
work; it is not a matched speed baseline against the service run. Full independent
attribute decoding and matched batch/query costs remain open. Do not compare these
times with RPKI or the million-snapshot stress benchmark.

0.5×/1×/2× source-time-paced replay and three-repeat matched experiments are
designed future Phase 20 work, not executed. Before expansion: independently
constrain the complete output projection, verify each selected ledger, budget
the actual larger-window disk/RAM/time, and preregister a new cap. Negative cost
or capacity findings remain valid; correctness failures stop the stage.

## Reproduction

Install the locked `ris`, Kafka, Spark and lake extras; Java 21 is required.
No normal CI downloads archives. Explicit local commands:

```sh
.venv/bin/python -m workloads.ris_framing_audit --output .cache/phase18/new-framing
.venv/bin/python -m workloads.ris_oracle --files 1 --output .cache/phase18/new-oracle
source .cache/local-kafka-env.sh
set -a; source .cache/minio.env; set +a
.venv/bin/python -m benchmarks.ris_service --files 1 --oracle .cache/phase18/new-oracle --output .cache/phase18/new-service --max-seconds 1800
```

Use fresh private directories and isolated topics/prefixes. `--files 3` or `12`
is a capability, not permission to skip the preceding correctness/resource gates.
Public results are aggregate metadata; raw archives/normalized events stay private.
