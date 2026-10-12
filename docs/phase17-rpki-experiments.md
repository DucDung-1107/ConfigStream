# Phase 17 — RPKI experiments and cost evidence

## Frozen input and rights

Reuse [Phase 13 source selection](../workloads/results/dataset-feasibility/rpki.json):
`ripencc.tal`, 2026-09-01 through 2026-09-14. All fourteen cached compressed
archives were SHA-256/size verified; no new source download occurred.
Exact URLs and hashes are in [source manifest](../workloads/results/phase17/source-manifest.json).

The [RIPE publisher description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md)
documents validated VRP CSV archives and trust-anchor scopes. Its historical
validator changelog is not proof of the actual collection build for this window.
The [RIPE copyright statement](https://www.ripe.net/about-us/legal/copyright-statement/)
provides general research/educational-use conditions and attribution obligations;
third-party rights are separate. No archive-specific unrestricted redistribution
license was established. Private bounded research analysis is the declared use;
raw/normalized publication remains blocked pending permission verification. Git
contains owned fixtures and aggregate hashes/counts only, not source rows.

## Environment and budgets

macOS 27.0 arm64, Apple M3 Pro (12 cores), 18 GiB physical RAM, Python 3.12,
Java 21 and Spark 4.0.2. Starting free disk: 675,971,788,800 bytes.
Docker/Compose executable and container limits were unavailable; localhost
Kafka `9092` and MinIO `9000` refused connections. No system infrastructure was
installed or reconfigured. Broker/runtime versions cannot be measured without
services. Existing compose definitions are not running-service evidence.

Spark local runs used `local[2]`, 2 GiB driver heap and 8 shuffle partitions.
Sampling began after session startup, every approximately 2 seconds. Process-tree
RSS is a sum, potentially double-counting shared pages; it is not JVM heap usage
or exact peak physical RAM. Limits: 12 GiB sampled RSS, at least 20 GiB free disk,
15 GiB per-run growth, 50 GiB experiment output. No concurrent heavy real-data
Spark run, no paid query and no deletion of prior evidence or volumes occurred.

## Measured stages — not a distributed end-to-end acceptance

| Stage | Actual experiment | Result / limitation |
|---|---|---|
| A | Owned CSV, temporal, Spark, checkpoint and publication fixtures | Offline correctness passed |
| B | 1,024 real rows, private local prototype pilots | No complete day; pre-release raw policy, not final ledger |
| C | One full day; repeated in final two-day run | 360,226 members; exact oracle match; local Parquet only |
| D | Two full days, final query `00000000-0000-4000-8000-000000000031` | 720,732 rows, 717 additions + 437 removals; exact local reconstruction |
| E | Independent batch oracle for all fourteen days | 5,062,923 rows, 14 exact sets, 13 boundaries; full Kafka/Spark/MinIO run NOT_RUN |

The final local run reused the independently verified day-one canonical state
from the earlier run, but wrote/revalidated both days' **final-policy raw ledgers**.
This is a warm/reuse execution, not a cold first-ingestion benchmark. Input
groups were ordered by observation ID rather than assuming chronological arrival.
All fourteen oracle daily sets/differences were compared against the unchanged
Phase 13 measurements. No discrepancy or unexplained exclusion was found.

### Negative execution evidence

An initial local caller submitted a whole day's 1,408 chunks to one callback.
The 512-chunk safety guard rejected it before raw publication. The caller was
corrected to groups of at most 256 chunks; limits were not raised. Private failure
logs are retained. Prototype ledgers predating `full-source-row-v1` are deliberately
not accepted by the final raw reader; they are retained, not rewritten. The
observation-bound row identity amendment also prevents identical bytes on two
dates being mistaken for retry. Test collection filename collision and an invalid
negative-fixture construction were corrected before final regression.

## Preliminary costs

Final two-day local run:

- Source preparation: 89.5707 s; 134,412,108 serialized JSON value bytes.
  This is the Kafka **projection**, not measured network/broker bytes.
- Twelve bounded local callbacks: 394.7633 s, including reconciliation, sorting,
  validation, publication and transition work. Approximately 1,825.73 logically
  accepted source rows/s over that denominator, **not native event velocity**.
- Elapsed through publication: 487.2666 s; external wrapper including final
  reconstruction/session shutdown: 499.49 s. Startup and query costs are not
  interchangeable with callback processing time.
- Peak sampled process-tree RSS: 2,461,007,872 bytes (~2.29 GiB), 232 samples.
  Minimum free disk: 675,411,996,672 bytes. Guard failure: none.
- Final-policy source Parquet: 164,369,346 bytes / 96 files. Two canonical states:
  69,888,316 bytes / 16 files. Transition Parquet: 205,191 bytes / 4 files.
  These are separate physical datasets, not a production compression ratio.
- All cached outputs/oracle/prototypes: peak sampled 1,661,054,190 bytes (~1.55 GiB).
  Raw archives remain in the existing Phase 13 cache, retained once.

Canonicalization/deduplication/anti-joins are distributed DataFrame work. Globally
sorted digest streams use `toLocalIterator(prefetchPartitions=False)`; memory is
bounded by the largest partition, not guaranteed constant. No full-state
`collect()`, `toPandas()` or millions-row driver dictionary is used. Serial hashing,
re-reading cumulative active-day ledgers and increasing callback duration are
known cost bottlenecks. Live defaults are 64 chunks/batch, unlike the local
256-chunk groups; these measurements do not predict Kafka/Spark throughput.

### Independent batch baseline

All fourteen states, exact SQL differences and every reconstructed state were
validated by SQLite and exported to private Snappy Parquet. Fresh first pass:
66.06 s wrapper, 142,458,880-byte parent maximum RSS; subsequent resumed export
including row-coordinate maps: 67.23 s, 145,604,608-byte parent maximum RSS.
These are distinct runs, **not one end-to-end total**. The second pass briefly
overlapped fixture testing. Member Parquet totals 443,213,860 bytes; row maps
81,907,245 bytes; SQLite 358,998,016 bytes. They preserve different columns and
omit transport/Kafka/recovery work, so no matched engine superiority claim follows.
Phase 20 must align projection, cache state, query and correctness requirements.

## Reproduction

Existing locked dependencies and a verified private Phase 13 cache are required.
Normal CI uses only owned fixtures. Commands do not download archives or start
services. Raw results/checkpoints stay ignored.

```sh
make check
make api-test
make spark-test
make rpki-oracle
make rpki-local RPKI_ARGS='--days 1 --query-id 00000000-0000-4000-8000-000000000030'
make rpki-local RPKI_ARGS='--days 2 --query-id 00000000-0000-4000-8000-000000000031'
make rpki-reconstruct RPKI_ARGS='--days 2'
```

The committed compact results derive from the private oracle summary, local
measurements/reconstruction/resource samples and committed-file inventories.
Physical bytes are `sum(stat().st_size)` over selected `*.parquet`, not source
logical bytes; exact ledgers remain private. Counts/digests can be reproduced
without publishing external records.

Only after service/legal/resource gates pass, execute the owned live smoke and
advance one complete day, two consecutive days, then fourteen:

```sh
RUN_PHASE17_SERVICES=1 make phase17-integration-test
make rpki-publish RPKI_ARGS='--days 1'
make rpki-stream RPKI_ARGS='--days 1'
make rpki-reconstruct RPKI_ARGS='--days 1 --minio'
# Repeat with --days 2, then --days 14, and NEW --query-id UUIDs for stream.
```

Configure existing Kafka/MinIO environment variables first; use an isolated lake
prefix and retained topic. The importer verifies/resumes each acknowledged day.
Use a new local query UUID when changing the selected catalog: local callback
batch IDs are assigned to the selected groups, not globally across experiments.
The fixed real-data topic must contain only the declared reference selection;
unrecognized observations fail closed rather than being silently filtered out.
`availableNow` drains already available Kafka input; producer completion and a
drained query alone do not complete a day. Missing source rows remain pending.
The 1,800-second query cap is a guard, not an asserted fourteen-day processing
time. Stop on identity/oracle disagreement, invalid transitions, resource breach
or unavailable committed state. No DNS/RIS job, full fourteen-day service replay,
million-snapshot benchmark or Phase 18 was executed.
