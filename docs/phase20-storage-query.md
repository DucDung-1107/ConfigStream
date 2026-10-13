# Phase 20 — storage and query trade-offs

## E2R: matched, lossless evaluation projection

[Trials and preference decisions](../workloads/results/phase20/rpki-representations.json)
contain all three builds per R1/R2/R3 in seed-42 order, with three reused-process
query cycles per build. All fourteen daily sets and thirteen added/removed tuple
sets compare exactly, ordered row-for-row, with the independent read-only SQLite
oracle. Query output digests agree across all configurations, not merely totals.

The canonical tuple string reversibly yields scope/ASN/prefix/maxLength; frozen
membership-v1 deterministically yields membership identity. Dates, complete-state
identities, source-row coordinate maps, source artifacts and provenance remain
shared references, counted once per candidate. This is a local evaluation
representation, not a production lake migration or altered membership policy.

R1 stores fourteen full sets plus common transition sidecars. R2 stores day1 plus
those same thirteen boundaries. R3 adds a day8 checkpoint, seven accepted days
after initial day1. Snappy and 65536-row group targets are identical. Empty action
sidecars remain explicit; missing or invalid removal fails, rather than empty read.

| Candidate | Analytical bytes | Files | Saving vs R1 | Preference |
|---|---:|---:|---:|---|
| R1 | 47,756,423 | 40 | reference | reference |
| R2 | 3,555,516 | 27 | 92.55% | PREFERRED |
| R3 | 6,954,890 | 28 | 85.44% | PREFERRED |

Median seconds below are median of three build-level medians; every build retains
its three individual warm cycles. Query time includes Parquet reads, SQLite
materialization and independent exact output validation. Import/startup is
NOT_MEASURED, build time is separate. OS cache was not evicted; these are not cold
filesystem measurements or production indexed-query latency.

| Required query | R1 | R2 | R3 |
|---|---:|---:|---:|
| all_days | 8.2221 | 8.3477 | 8.1552 |
| asn_transitions | 1.2618 | 1.3080 | 1.2421 |
| daily_transitions | 1.2889 | 1.2355 | 1.2309 |
| membership | 0.3879 | 0.4040 | 0.3872 |
| selected_day | 0.5741 | 0.5855 | 0.5832 |

Both candidates meet >=20% analytical saving AND <=2x median time on **every**
registered query. **H2 SUPPORTED_WITHIN_TESTED_SCOPE**, not a universal preference
or speedup. Build medians: R1 3.834 s,
R2 1.797 s,
R3 1.838 s.
All values/ranges/paired ratios are retained; no incorrect run is averaged.

## Analytical versus all-in accounting

[Local all-in inventory](../workloads/results/phase20/rpki-all-in-storage.json)
counts unique source archives, exact coordinate-map files, shared source/daily/
transition metadata, candidate analytical files, telemetry/results and retained
query scratch. Sources/maps are not multiplied per query. File st_blocks is
reported separately, not exclusive APFS allocation or physical query I/O.

[Read-only deployed inventory](../workloads/results/phase20/storage-inventory.json)
measures accepted RPKI/RIS/DNS MinIO prefixes, Kafka selected directory apparent/
allocated bytes and checkpoint content/allocated bytes. Broker costs are not
omitted or assigned invented zeroes to favor streaming. These snapshots include
indexes/preallocation unlike earlier broker-reported partition-log sizes; do not
equate the two measures. MinIO backend allocation/global service files are
NOT_MEASURED. A listing proves object existence, not a new logical identity audit.

## E2D/E2B/E2T scope

DNS [prior component comparison](../workloads/results/phase19/representation-costs.json)
remains unchanged: reused components are **2,207,468 bytes larger**. All source
rows, status/time/worker and contributor references are required. Standalone
D1/D2/D3 matched builds/query repetitions are NOT_RUN; no new preference claim.

RIS preserves all 4,725,156 native occurrences and 57 exclusions in accepted
retained history. Per-minute summaries would be lossy auxiliary views, not log
replacement. New nested representation/query repetitions are NOT_RUN.

Terraform [read-only reference](../workloads/results/phase20/terraform-reference.json)
rechecks 10,188 content hashes, 6,505 ordinal incarnations and the exact 3,683-edge
ledger. Three selected lookup/metadata cycles pass. No new mining, deployment
interpretation or raw-source reconstruction from normalized text is claimed.

Reproduce authorized bounded E2R only:
```sh
source .cache/local-kafka-env.sh
caffeinate -i .venv/bin/python -m benchmarks.phase20 --suite rpki --output .cache/phase20/rpki-new
.venv/bin/python -m benchmarks.phase20_report --private .cache/phase20
```
Fresh isolated output required; no automatic long service run.
