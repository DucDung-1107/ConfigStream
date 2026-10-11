# Dataset methodology

## Categories and source

| Category | Role | What is real? |
| --- | --- | --- |
| A: public Batfish configurations | syntax/size fixtures | Public educational/test configuration files, not production measurements |
| B: controlled histories | reproducible changes and ground truth | Baseline content only; assets, times and changes are generated |
| C: original generator | extreme/boundary stress | Entirely synthetic; the Phase 2 generator remains unchanged |

Source: [batfish/batfish](https://github.com/batfish/batfish), pinned revision
`540a4644e0245adffc5d51ec911411a5cd09aa0d`.
[License at that revision](https://github.com/batfish/batfish/blob/540a4644e0245adffc5d51ec911411a5cd09aa0d/LICENSE):
Apache-2.0. Downloaded 2026-10-11; source license is retained alongside the
ignored local corpus. Only one attributed 108-byte Apache-licensed parser fixture
is redistributed under `tests/fixtures/phase10`; the external corpus stays ignored.
The importer selects regular archive members below `/configs/`, not a broad GitHub crawl.
The entire archive is SHA-256 pinned in `workloads/manifests/batfish.json`.

Measured locally from that archive (not estimated):

| Metric | Value |
| --- | ---: |
| Archive bytes | 9,386,252 |
| Candidate configuration files | 748 |
| Candidate configuration bytes | 824,149 |
| Usable IOS-style files | 324 |
| Unique usable content hashes | 268 |
| Usable bytes | 361,177 |
| Skipped potentially credential-bearing files | 58 |
| Skipped unsupported syntax files | 366 |
| Size min / median / p90 / p95 / max | 108 / 557.5 / 3302 / 3837 / 8015 bytes |
| Size mean | 1114.7438271604938 bytes |

These are mostly small parser/lab examples. They improve syntax diversity but are
**not evidence of production-sized device configurations or observed operational history**.
No vendor-semantic validation is claimed. IOS-style recognition is conservative
text recognition (`hostname` plus a supported interface spelling), not a vendor parser.
Repeated content across paths is retained in the file distribution; sampling clones
baselines with replacement. This low-entropy reuse limits interpretation of compression
and does not establish production configuration sizes.

## Import security and provenance

`make dataset-fetch` verifies the archive checksum, rejects unsafe archive paths,
never extracts links, and limits downloads to 250 MB/files to 2 MB. Non-UTF8,
empty, unsupported and potentially credential-bearing files are skipped. Filtering
is intentionally overinclusive, including test password/secret/token/SNMP keywords;
it is not a universal secret detector. Never print imported configuration bodies.

The local measured manifest contains sorted original relative paths, byte counts,
per-file checksums, source revision/license and an aggregate checksum. Workload
sidecars retain this provenance. Elasticsearch/API projections remain metadata-only.

## Semi-synthetic histories

`make workload-generate WORKLOAD_ARGS='--assets 100 --versions-per-asset 10 --seed 42'`
writes ignored `.cache/workloads/history.jsonl`, a manifest and a ground-truth JSONL
sidecar. Transport records reuse Asset/ChangeEvent/ConfigSnapshot and the existing
envelope. Version 1 contains an unchanged real baseline; later versions apply
controlled IOS-style logging policy / DNS edits. Nginx timeout and generic-service
timeout edits are also supported by the pure mutation function, tested locally.
This is not full vendor interpretation; unsupported syntax is not mutated.

Profiles:

- `normal-maintenance`: asset changes spread across each logical day.
- `fleet-rollout`: one middle version changes the fleet at the same logical time.
- `incident-remediation`: correlated middle change, followed by fleet rollback 15
  logical minutes later under the same campaign ID.
- `stress-replay`: normal histories replayed faster, never a physical device-frequency claim.

Logical days and physical publishing time are distinct. Baseline sampling follows
the measured corpus distribution; small/medium/large buckets are measured size
terciles, not invented size targets. No meaningless padding is used.

Ground truth records asset/version, baseline path/hash, mutation/group, changed-line
count, deterministic campaign ID and logical time. It is evaluation-only, never input
to the diff engine. IDs, choices and event times are seeded; ingest time is event
time plus 5 ms. Optional bounded delivery-window shuffling and exact snapshot-ID
duplicates do not modify canonical histories. Source manifest and workload config
must be retained for replay. No wall-clock time enters workload identity.

Limitations: one curated repository, no observed Git history, small lab baselines,
syntax heuristics and two IOS-style mutation intents. Semi-synthetic results must
always be labelled as such. Broader vendor/large real corpora are future work.

## Measured Phase 10 dataset scale

| Dataset | Type | Files/assets | Real source-pool bytes | History | Generated snapshots |
| --- | --- | ---: | ---: | --- | ---: |
| A: pinned Batfish selection | Public test/lab configurations | 324 files / 268 unique contents | 361,177 | Baselines only, not observed history | Not separately replayed |
| B: main-1m-2g-50k-20261011 | Controlled semi-synthetic | 10,000 cloned assets | Same 361,177-byte pool | 100 versions, 100 simulated UTC dates | 1,000,000 |
| B: JSONL reproducibility demo | Controlled semi-synthetic | 100 cloned assets | Same source pool | 10 versions | 1,000 |
| C: original Phase 2 generator | Fully synthetic | Configurable | Not applicable | Deterministic templates/mutations | No new Phase 10 scale run |

Main input content totals 1,144,675,896 bytes (mean 1144.675896 bytes/snapshot),
not 1 GB of distinct public originals. All 324 paths/268 unique hashes were sampled
with replacement. Main raw/processed committed Parquet totals 876,030,173 bytes.
See [actual rates, failures and limitations](benchmark.md). Both JSONL demo runs,
ground-truth sidecars and manifests were byte-identical; they are ignored locally.
