# Phase 10 acceptance record

Local acceptance recorded on **2026-10-11**, using one machine.
Repository: [kongwoang/ConfigStream](https://github.com/kongwoang/ConfigStream).
This phase adds datasets, historical Spark jobs and measured replay experiments;
it does not add a dashboard, deployment platform, live collectors or ML.

## Delivery and files

Completed delivery commits before the final benchmark commit:

- `9ab22975d24dde9e8caa220e22cd8d2a959ea113` — real configuration workload integration.
- `68a97de951cc38207c21c2d63d45a89b7f67f930` — historical Spark analytics.
- The final `feat: add reproducible scale benchmark` SHA and CI URL are provided
  in the completion hand-off / GitHub commit checks, not embedded in its own content.

Created:

- `workloads/{__init__,corpus,history,main}.py`, pinned and measured
  `workloads/manifests/batfish*.json`, `workloads/rules.yaml`.
- `analytics/{__init__,config,jobs,main}.py`.
- `benchmarks/{__init__,main,metrics,report}.py`; 21 compact measured run JSONs,
  verification JSON, summary CSV and three generated SVGs in `benchmarks/results/`.
- `tests/unit/test_workloads.py`, `test_analytics_config.py`,
  `test_benchmarks.py`; `tests/spark/test_analytics.py`;
  `tests/phase10_integration/test_history.py`.
- Four attributed fixture files under `tests/fixtures/phase10/`:
  README, Apache license, manifest and a 108-byte configuration.
- `docs/datasets.md`, `docs/historical-analytics.md`, `docs/benchmark.md`,
  this acceptance record.

Modified existing files: `.env.example`, `Makefile`, `README.md`,
`pyproject.toml`, `docs/architecture.md`, `docs/data-lake.md`,
`spark/common/config.py`, `spark/common/runtime.py`,
`spark/streaming/sinks/lake.py`.

No new dependency was required; the existing pinned Kafka/Spark/lake extras and
standard library cover the implementation. Wheel packaging includes the three
new packages. `uv.lock` is unchanged because dependency resolution is unchanged.
CI's existing unit/API and Spark jobs discover the new tests automatically;
external downloads and large/service benchmarks do not run in ordinary CI.

## Public corpus and methodology

One substantial curated source was integrated:
[Batfish configurations](https://github.com/batfish/batfish/tree/540a4644e0245adffc5d51ec911411a5cd09aa0d),
revision **540a4644e0245adffc5d51ec911411a5cd09aa0d**,
[Apache-2.0 license](https://github.com/batfish/batfish/blob/540a4644e0245adffc5d51ec911411a5cd09aa0d/LICENSE),
retrieved **2026-10-11**. A fresh urllib download reproduced the pinned archive
checksum, discovered files and measured manifest.

| Measurement | Actual value |
| --- | ---: |
| Compressed repository archive | 9,386,252 bytes |
| Candidate regular files below /configs/ | 748 |
| Candidate configuration bytes | 824,149 |
| Usable IOS-style files | 324 |
| Distinct usable content hashes | 268 |
| Usable baseline bytes | 361,177 |
| Skipped files | 424 |
| Potential credential keyword skips | 58 |
| Unsupported syntax skips | 366 |
| Min / median / p90 / p95 / max bytes | 108 / 557.5 / 3302 / 3837 / 8015 |
| Mean bytes | 1114.7438271604938 |

Archive SHA-256:
`791ce74605602dde03d4f44b52b2106bced8ff8e43dd1b605f3759436e123ed7`.
Measured corpus-entry SHA-256:
`a46694c94f684cdc6811a14f3cd1f209543c71bf0279da769813a0dc8990894b`.

These are real **public parser/lab configuration files**, not observed production
devices, production-sized files or operational history. The skipped category is
conservative text filtering, not proof that 58 actual credentials were discovered.
The full archive stays ignored and may contain skipped public test material.

Dataset A is the curated baseline corpus. Dataset B clones baselines and generates
controlled histories. Dataset C is the unchanged original synthetic generator;
no new C scale benchmark was run. Baseline reuse means the main fleet has 10,000
generated assets but only 324 original paths / 268 distinct contents.

Provenance is in the measured corpus manifest and workload sidecars: source URL,
revision, license, checksum, original relative path/hash, profile, versions,
campaign ID and logical time. No external domain schema was changed.
Baseline v1 is unchanged original text and has a config_observed event.
Later versions transform previous content and use existing Asset, ChangeEvent and
ConfigSnapshot models. The streaming engine, not ground truth, derives diffs.

Controlled IOS-style intents are logging-policy toggles and DNS updates using
documentation addresses. Nginx/generic timeout changes are fixture-tested.
No unsupported vendor interpretation is claimed. Profiles:
normal-maintenance (one version per asset/day, spread across hours);
fleet-rollout (correlated middle version); incident-remediation (correlated
change/rollback 15 simulated minutes apart); stress-replay (accelerated history).
Campaign/snapshot/asset IDs and times are deterministic. Version 1 is a baseline,
not a fake diff. No padding is used.

Ground-truth JSONL records asset, previous/new version, mutation/group, intended
line-change count, campaign, logical time and source path/hash. Delivery can shuffle
bounded windows and repeat exactly the same snapshot ID. Ratios are probabilities,
not guaranteed exact realized percentages. JSONL records, truth and manifests for
two 100-asset × 10-version executions were byte-identical.

## Historical analytics and output contracts

Input is exclusively frozen, validated committed lake manifests through
`read_committed`. No partial-object recursive read, Elasticsearch history scan
or driver collection of complete historical DataFrames is used.
IDs are deduplicated across committed input; diagnostics count occurrences.
Configuration bodies/details are dropped before native DataFrame aggregations.

Eight typed Snappy Parquet outputs under `analytics/<job>/run_id=<sha256>`:

| Job | Main output schema |
| --- | --- |
| change-frequency | asset_id, UTC window_start/end, snapshot/diff/alert counts (long) |
| fleet-activity | UTC window, three counts and distinct changed-asset count |
| diff-magnitude | diff_count; mean (double), approximate p50/p90/p95 and max for added/removed/changed |
| alert-trends | severity, rule_id, asset_id, UTC window, count |
| diagnostic-trends | safe type, nullable UTC window, count |
| asset-summary | asset, first/last event time, counts, high/critical counts, mean/max changed |
| anomalies | asset/window, diff_count, prior count/mean/stddev, nullable z-score, boolean flag, policy |
| fleet-bursts | fleet window, distinct changed assets and the same score/flag fields |

Snapshot counts include baselines; diff counts count persisted comparisons,
including zero-magnitude normalized comparisons. Alerts are rule matches,
not confirmed incidents. Undated diagnostics remain in null-time buckets unless
an explicit time filter excludes them.

Run identity hashes job version batch-v1, canonical input manifests and normalized
parameters. Time filters are UTC, start inclusive/end exclusive. Equivalent integer/
floating-point thresholds now hash identically; the CLI and pure API reused the same
existing commit in two repetitions. A pre-fix equivalent CLI run is retained in the
local lake for audit, not treated as new input history. The completion marker is
written only after selected outputs succeed; reruns reuse it. Concurrent writers
with identical job/run IDs are not supported.

Anomaly formula: current count minus prior mean, divided by prior population standard
deviation. Use at most seven prior windows, never future windows; require at least
three prior windows, count >= 5, z >= 3. Zero-variance excess above the prior constant
can flag once minimum count/history is met, with null z-score. Insufficient history
does not flag. Idle windows are filled with zero within observed bounds.
Fleet scoring uses the same formula on distinct changed assets.

Controlled fixture acceptance: 36 snapshots, 30 adjacent diffs, 12 alerts,
one hourly fleet burst at 2026-01-03T00:00:00Z.
Full-corpus rollout: 600 snapshots, 500 diffs, 200 alerts; 100 assets/diffs in its
known DNS campaign window, exactly one detected hourly burst using minimum count 10.
Ordinary maintenance has 4–5 assets/hour and stays below that minimum.
This is controlled ground truth, not accuracy on real security incidents.

Manifest/date-path pruning is not implemented; filters bound results, and
deduplication may limit predicate pushdown. Long hourly dense-window expansions
can be expensive. Fleet scoring orders its single global time series in one
partition, unlike parallel per-asset scores. No historical batch job is put in FastAPI.

## Benchmark architecture and measured results

A command-driven harness reuses the existing producer and direct native Spark
streaming query. One Asset per member is published, followed by snapshots only
(audit ChangeEvents are outside primary throughput scope). Kafka alerts remain on;
search/API are off. Each ID has new start offsets, checkpoint, query identity and
lake prefix. An advisory publisher lock prevents overlapping benchmark processes;
external publishers still require operator coordination.

The harness captures monotonic publish/drain/job times, native progress JSON,
source offsets/lag, state/drop metrics, Java and container samples, content/value
bytes, committed Parquet bytes, status and failures. It uses bounded deadlines,
Ctrl-C cleanup and a 2 GB VM disk reserve. It does not delete committed data.

See [all measured runs](../benchmarks/results/summary.csv),
[definitions/tables/plots](benchmark.md) and
[independent correctness checks](../benchmarks/results/correctness-verification.json).
All **21 executions** are retained: **17 completed, four failed**.

Machine: Apple M3 Pro, macOS 27.0 arm64, 12 host cores, 18 GiB RAM.
Docker engine reports 2 CPUs / 6,189,654,016 RAM bytes; Docker 29.8.1.
Python 3.12.14, Java 21.0.12.1, Spark 4.0.2 / Scala 2.13, Kafka 4.1.2.
Physical storage device was not probed. This is a single-machine experiment.

Main configuration: 10,000 assets × 100 versions, seed 42, stress-replay,
target 1000 snapshots/s, local[2], six snapshot partitions, 2g driver heap,
50,000 offsets/trigger, 48-hour watermark, 87,600-hour state timeout,
32 versions/asset, logical start 2026-01-01T00:00:00Z.
The long timeout is a benchmark policy, not a production recommendation.

Main actual results:

| Metric | Measured result |
| --- | ---: |
| Input and accepted snapshots | 1,000,000 |
| Adjacent diffs | 990,000 |
| Rule alerts | 250,000 |
| State-eviction diagnostics | 680,000 |
| Logical raw content bytes | 1,144,675,896 |
| Serialized snapshot value bytes | 1,622,093,126 |
| Committed raw/processed Parquet bytes | 876,030,173 |
| Producer rate | 999.9994 snapshots/s |
| Streaming/drain time | 1065.618 s |
| Logical accepted rate including drain | 938.422 snapshots/s |
| Native mean processedRowsPerSecond | 2678.369 rows/s |
| Active trigger p50 / p95 | 48,026 / 49,932 ms |
| Maximum exposed operator rows | 89,997 |
| Maximum exposed state memoryUsedBytes | 17,510,807 |
| Peak sampled offset lag / final lag | 99,593 / 0 |
| Sampled Java peak RSS | 2,487,205,888 bytes |
| Sampled Kafka / MinIO peak memory | 1,017,223,577 / 487,797,555 bytes |
| Eight persisted batch-job timers summed | 85.121 s |

Native rows/s counts three source branches and must not be advertised as snapshots/s.
Trigger duration is not simulated-event-time end-to-end latency.
Operator memory is not complete pipeline RAM. Physical Parquet covers multiple
projections and is not a raw-only compression ratio.

Rate series uses 100/500/1000 snapshots/s at 10k snapshots. The comparable 500/s
configuration has three repetitions (median 33.857 s streaming wall).
A local[4] comparison is descriptive, with one repetition, and retains two shuffle
partitions. Main has one repetition; no 5M+ run or rate above 1000/s was attempted.
Three tier commands exist, but support is not evidence a stress tier completed.

Warm post-streaming batch-job timer sums at 10k / 100k / 1M:
8.134 / 27.041 / 85.121 s. Main change-frequency / asset-summary /
alert-trends / asset-anomaly timings: 14.427 / 11.108 / 1.813 / 17.690 s.
These include output count/write, not cold JVM/read startup; shapes/file counts vary.

Failures retained:
initial native-progress parsing bug; first million target's 1g Java heap exhaustion
(600,358 attempted, 200,110 committed); controlled late-data completeness failure
(423 dropped, 577 accepted of 1000); small-fleet capacity-floor failure
(271 evicted_version, 159 unresolved gaps, 9729 accepted of 10k).
The successful main changes both heap and batch cap, so no isolated attribution.
The default 500 × 20 smoke passed; grouped iteration is not guaranteed sorted,
and retaining only 32 versions can reject older neighbors when a batch spans more.
These findings are limitations, not removed saturation points.

## Regression and acceptance commands/results

| Command | Final passing cases |
| --- | ---: |
| make check | 711; Ruff lint/format passed |
| make api-test | 80 |
| make spark-test | 58 |
| make analytics-test | 21 (overlaps the unit/Spark cases above) |
| make integration-test | 3 |
| make spark-integration-test | 3 |
| make lake-integration-test | 1 |
| make search-integration-test | 2 |
| make api-integration-test | 2 |
| make phase10-integration-test | 1 |

**861 unique passing cases** across disjoint suites; analytics-test is not added
again. All original Phase 0–9 cases remain; none was weakened/deleted.
One existing TestClient deprecation warning remains in API tests.

First two Phase 10 GitHub Actions runs passed:
[workloads](https://github.com/kongwoang/ConfigStream/actions/runs/38115335833),
[analytics](https://github.com/kongwoang/ConfigStream/actions/runs/38115746694).
The final benchmark commit's Python/API/Spark checks are the final hand-off gate;
its exact workflow result/URL is supplied with the commit SHA.

Executed service/setup and correctness commands (logs ignored under .cache/phase10):

```sh
. .cache/local-kafka-env.sh
set -a; . .cache/minio.env; set +a
limactl start k
make api-setup
make lake-up
make kafka-topics
make lake-init
make search-up
make search-init
make dataset-fetch
make dataset-inspect
.venv/bin/python -m workloads.main fetch --corpus .cache/datasets/batfish-repro
make workload-generate WORKLOAD_ARGS='--profile fleet-rollout --assets 100 --versions-per-asset 10 --seed 42 --output .cache/workloads/phase10-real-a.jsonl'
make workload-generate WORKLOAD_ARGS='--profile fleet-rollout --assets 100 --versions-per-asset 10 --seed 42 --output .cache/workloads/phase10-real-b.jsonl'
cmp .cache/workloads/phase10-real-a.jsonl .cache/workloads/phase10-real-b.jsonl
cmp .cache/workloads/phase10-real-a.truth.jsonl .cache/workloads/phase10-real-b.truth.jsonl
cmp .cache/workloads/phase10-real-a.manifest.json .cache/workloads/phase10-real-b.manifest.json
```

Main and controlled experiment commands:

```sh
.venv/bin/python -m benchmarks.main --benchmark-id main-100k-20261011 --assets 1000 --versions-per-asset 100 --rate 1000
.venv/bin/python -m benchmarks.main --benchmark-id main-1m-20261011 --assets 10000 --versions-per-asset 100 --rate 1000
.venv/bin/python -m benchmarks.main --benchmark-id main-1m-2g-50k-20261011 --assets 10000 --versions-per-asset 100 --rate 1000 --driver-memory 2g --max-offsets 50000
.venv/bin/python -m benchmarks.main --benchmark-id final-rollout --assets 100 --versions-per-asset 6 --rate 500 --profile fleet-rollout
.venv/bin/python -m benchmarks.main --benchmark-id final-duplicates-ooo-wide --assets 50 --versions-per-asset 20 --rate 500 --duplicate-ratio 0.05 --out-of-order-ratio 1 --max-reordering-distance 150
.venv/bin/python -m benchmarks.main --benchmark-id final-late-controlled --assets 100 --versions-per-asset 10 --rate 500 --out-of-order-ratio 1 --max-reordering-distance 300 --max-offsets 100 --watermark '1 seconds'
make benchmark-smoke BENCHMARK_ARGS='--benchmark-id make-smoke-wrapper --assets 100 --versions-per-asset 100 --rate 1000'
make benchmark-smoke BENCHMARK_ARGS='--benchmark-id make-smoke-final --rate 1000'
make benchmark-report
make benchmark-report BENCHMARK_REPORT_ARGS='--input benchmarks/results --output .cache/benchmark-report'
```

The final controlled rate loop used 500 assets × 20 versions, local[2], with
IDs final-smoke-100-r1, final-smoke-500-r1/r2/r3 and final-smoke-1000-r1;
each was invoked via benchmarks.main with its corresponding --rate.
Earlier prototype commands/configs and statuses are retained in the compact JSON.
An invalid '1seconds' CLI request was rejected before execution; it is not a
measured run. Older policy/unrecorded-progress runs cannot be assumed byte-identical
under the latest workload identity policy.

The rollout analytics CLI ran twice with its captured query UUID:

```sh
CONFIGSTREAM_LAKE_PREFIX=benchmarks/final-rollout SPARK_MASTER='local[2]' make analytics-run ANALYTICS_ARGS='--query-id fad8ece4-c54d-4f98-9f0b-eaa09b050a26 --window "1 hour" --minimum-count 10'
CONFIGSTREAM_LAKE_PREFIX=benchmarks/final-rollout make analytics-inspect
```

Use the actual query UUID from the result JSON; the captured value is verified in
the command receipt before commit. Native metadata verification used joins from
persisted old/new snapshot IDs back to snapshot asset/version and counted any
missing endpoint, cross-asset match or nonadjacent version. Main and reordered runs
had zero invalid pairs. Ground-truth delivery records showed 339 below-prior-maximum
version arrivals in the wide reordered experiment.

Committed tables/three SVGs regenerate byte-identically from committed compact
JSONs without services or corpus download. Reused analytics commits match exactly
between repeated CLI execution and the pure API.

## Limitations, safe claims and next phase

Safe statements:
- public lab configurations provide measured syntax/size diversity;
- seeded mutations provide controlled semi-synthetic histories/ground truth;
- native Spark state, adjacent pairing and committed batch analytics work locally;
- this local run completed one million snapshots / 1.145 GB logical content;
- measured heap/cap/watermark limits, storage and warm-job behavior are documented.

Do not claim observed production history, thousands of natural per-router changes/s,
synthetic counts proving real Big Data, cold/steady-state/production latency,
horizontal cluster scaling, total RAM from one operator metric, incident-detection
accuracy, exactly-once delivery, or a successful unexecuted 5M stress run.

**The high input rates represent accelerated replay/stress testing of accumulated
fleet configuration history, not expected per-device configuration frequency.**

Remaining limitations: one small low-entropy corpus, limited syntax/mutations, no
observed Git history, no manifest/date pruning, bounded-state capacity tradeoffs,
long benchmark-specific timeout, no total pipeline RAM accounting, few repetitions,
no isolated high-rate CPU/storage diagnosis and no multi-node evaluation.
MinIO remains authoritative; Elasticsearch/FastAPI remain safe metadata projections.

Recommended next phase: review retention/cap policies against sparse real histories,
add broader/larger curated or observed datasets, and repeat isolated workload/resource
experiments before optional read-only UI or deployment. None is implemented here.
