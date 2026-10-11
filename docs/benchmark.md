# Controlled local replay experiments

## Phase 12 real IaC replay (separate experiment)

All accepted histories from the deterministic TerraDS sample were replayed without
cloning: 10188 snapshots, 3683 adjacent diffs, 12278085 logical config bytes,
29402997 committed raw/processed Parquet bytes, zero alerts/diagnostics. Target
500/s achieved 500.023 publisher snapshots/s; completion including drain was
68.742 seconds (~148.205 logical accepted snapshots/s). Native Spark branch metrics
are not interchangeable with logical records. [Acceptance](phase12-acceptance.md)
records exact pairs, lag/state/resources and analytics output validation.

Original history covers about 10.83 years; physical replay takes about a minute.
The high input rates represent **accelerated replay/stress testing of accumulated
configuration history, not expected per-device configuration frequency** or
Terraform apply cadence. This small, high-uniqueness, date-partitioned observed run
is not a matched throughput comparison with the reused-content 1M stress run.
Phase 12 decision B retains the architecture-evaluation framing; estimates are
not an additional observed million-record benchmark.

This page preserves the Phase 10 measurements. Phase 11 adds separate
[observed and empirically calibrated replay experiments](workload-calibration.md)
and [external-validity assessment](external-validity.md); the million run remains
semi-synthetic, not newly relabelled observed history.

## What the experiment means

**The high input rates represent accelerated replay/stress testing of accumulated
fleet configuration history, not expected per-device configuration frequency.**

Scale comes from fleet size × versions × retention, correlated fleet campaigns,
audit/history, near-real-time processing and historical queries. A million generated
records alone does not establish a naturally high-volume production workload.
Public lab configurations provide syntax/size diversity; controlled mutations provide
ground truth. Neither generated histories nor their incident/rule matches are observed
production behavior. See [dataset measurements](datasets.md).

Use cases include audit, explaining changes, finding risky policy edits, reconstructing
pre/post-incident history, locating unusually active assets and correlated fleet rollouts.
Throughput is one engineering dimension, not the entire motivation.

## Harness and isolation

`benchmarks.main` reuses the existing Kafka producer and native Spark stateful query.
It publishes one Asset per fleet member and ConfigSnapshot records; the audit-event
topic is deliberately outside the primary throughput scope. Workload JSONL generation
can emit ChangeEvent too. Elasticsearch/FastAPI are OFF; MinIO and Kafka alerts stay ON.
Spark still uses direct `readStream` from Kafka, never a Python consumer feeding Spark.

Each explicit benchmark ID creates a fresh checkpoint and `benchmarks/<id>` lake
prefix. Initial partition offsets are recorded before publishing, so retained demo data
is excluded. The shared six-partition snapshot topic is not deleted or repartitioned.
Run only one publisher/benchmark/integration suite at a time. A local advisory lock
prevents concurrent benchmark processes; other publishers must be stopped separately.
Existing IDs are refused, preserving prior successes/failures.

Each result records source revision/checksum, seed/profile, asset/version counts, target
rate, logical start time, Spark master, offsets/partitions, watermark/state timeout,
buffer cap, checkpoint and lake prefix. Workload ground truth stays ignored locally.
The first measured implementation used `history-v1`; `history-v2` also includes logical
fleet shape/source in identity, avoiding ID reuse between different workload shapes.
Reordering/duplication do not change canonical IDs. Replay exactly the same policy/config
for byte-equivalent semantic records; rate/master do not change workload identity.

## Measurement definitions

- `logical_raw_config_bytes`: sum of snapshot content UTF-8 lengths before Parquet
  compression; duplicates count as input volume, not distinct accepted versions.
- `kafka_value_bytes`: serialized snapshot envelope UTF-8 bytes, including its newline;
  excludes keys, protocol framing, replication and other topics.
- `parquet_bytes`: physical `.parquet` bytes below **committed manifest paths only**,
  including raw, normalized, diff, alert and diagnostic projections. This is not a
  raw-only compression ratio. Commit JSON/checkpoints/Kafka storage are separate.
- Producer rate includes registry publishing, snapshot generation, pacing and final
  delivery flush. It is a measured client workload rate, not broker-only throughput.
- Logical processing rate = accepted snapshot count / streaming wall time, from
  query start/publishing until the final completed batch reaches captured end offsets.
  JVM/dependency/session startup is recorded separately. Initial empty-query setup is
  still included in streaming wall time. Do not call it steady-state capacity.
- Native Spark `inputRowsPerSecond` / `processedRowsPerSecond` are also retained.
  The unioned raw/valid/invalid branches can count each source record three times;
  **native reported rows/s is not logical snapshots/s**. Arithmetic mean batch rates
  are diagnostic, not an independently measured sustained capacity.
- Micro-batch p50/p95 uses active `triggerExecution` durations. It is **not end-to-end
  event-time latency**: event time represents simulated historical days. No p95
  end-to-end latency is claimed.
  The empirical p50 index is `floor((n-1)/2)`; p95 is `min(n-1, floor(n*0.95))`.
  Small smoke runs have only 3–4 active batches, so their p95 is effectively a
  maximum, not a statistically stable tail-latency estimate.
- Recent progress history captures completed batches. The first smoke runs used
  last-progress sampling, which could miss a final/short batch; their native-rate
  timing samples are incomplete. Committed counts, bytes and wall time remain valid.
- State row/memory/drop metrics are exactly what Spark exposes per operator. Maximum
  operator rows is not a total number of buffered versions across all operators.
- Lag samples compare broker latest offsets with completed Spark source end offsets,
  not Kafka consumer-group commits. Sampling may miss the peak; final drain is checked
  separately against the publisher's captured end offsets.
- Resource samples record Java-driver RSS/CPU and Docker Kafka/MinIO stats. Driver
  RSS excludes Python workers and is not complete pipeline RAM consumption.
  CPU samples use the tools' per-core percentage conventions, so values can exceed
  100%. Individual peak samples are not average utilization or additive memory totals.

## Workload/retention policy

Versions span simulated days, while replay takes minutes. The benchmark uses a
48-hour watermark and deliberately long event-time state timeout (87,600 hours),
with at most 32 versions per asset. This avoids forgetting prior versions during
accelerated historical replay; it is **not a production retention recommendation**.
The history buffer is still bounded. Histories with 100 versions generate expected
`state_eviction` diagnostics after the cap, not parsing errors. Default shuffle
parallelism remains 2 even when master is `local[4]`; this is local task-slot sensitivity,
not horizontal cluster scaling.

The cap bounds history, not just memory. Native grouped input iteration is not
guaranteed to be version-sorted. If a micro-batch delivers more versions of an asset
than its retention cap, older neighbors can be evicted before they are processed,
even with version-ordered Kafka production. The harness compares accepted/diff
counts with canonical ground truth and fails visibly rather than calling such a
run complete. Reduce the per-trigger volume or increase the bounded version cap
when evaluating a small fleet with long accelerated histories. Watermark-drop
metrics alone do not account for capacity-floor rejections.

`--start-time`, `--state-timeout`, `--max-versions-per-asset`, `--watermark`,
`--max-offsets` and `--max-reordering-distance` are explicit overrides. A shuffled
delivery buffer must span versions of an asset to test version reordering: shuffling
16 records in a 50-asset version-major history only reorders assets. The controlled
out-of-order experiment uses 150 records (three fleet versions) instead.

`SPARK_DRIVER_MEMORY` defaults to `1g`; `make benchmark-main` explicitly uses `2g`
and 50,000 offsets per trigger, the configuration that completed the local million
run. The limit is passed before PySpark launches its JVM, and effective maximum heap
is measured. It cannot resize an already-running JVM. Spark's
[driver-memory configuration](https://spark.apache.org/docs/4.0.2/configuration.html)
must take effect at JVM launch, not merely change a runtime setting.

## Commands

```sh
make api-setup
make dataset-fetch
make dataset-inspect
make workload-generate WORKLOAD_ARGS='--profile fleet-rollout --assets 100 --versions-per-asset 10 --seed 42'
make lake-up
make kafka-topics
make lake-init
make benchmark-smoke BENCHMARK_ARGS='--benchmark-id smoke-local --rate 500'
make benchmark-main BENCHMARK_ARGS='--benchmark-id main-local --max-seconds 1800'
make benchmark-report
```

Source ignored local service environment/credentials before service commands as
described in [data lake](data-lake.md). Parameters are explicit CLI options; do not
silently reuse an old checkpoint. Ctrl-C records interruption and preserves committed
history. Deadline/query/publisher failures are retained as failures. VM disk below a
2 GB reserve aborts the run. No cleanup deletes historical data. Inspect local
`.cache/benchmarks/<id>/result.json` for raw safe metrics/ground truth.

## Results and reproducible plots

Only actual runs belong in [summary.csv](../benchmarks/results/summary.csv) and the
small per-run JSON files. `make benchmark-report` regenerates these and three SVG plots:

- [Input rate vs observed logical processing rate](../benchmarks/results/throughput.svg)
- [Input rate vs active micro-batch duration](../benchmarks/results/batch-duration.svg)
- [Logical input bytes vs persisted batch-job runtime](../benchmarks/results/batch-analytics.svg)

To regenerate plots from the **committed compact measurements** after a fresh clone,
without downloading data or running services:

```sh
make benchmark-report BENCHMARK_REPORT_ARGS='--input benchmarks/results --output .cache/benchmark-report'
```

Graphs are local smoke/scale measurements, not production/distributed capacity claims.
Failures remain in the result table. Undefined measurements remain null, never zero
or invented numbers. Analytics timings include count plus typed Parquet write for
each of eight jobs. Reused analytics commits do not constitute new timing measurements.

Dependency artifacts were cached by existing acceptance/small correctness runs before
scale experiments. No explicit statistical steady-state warmup interval is removed.
Small repetitions and `local[2]`/`local[4]` comparisons are descriptive; expensive main
runs may have one repetition. Do not infer significant differences from single samples.

`make benchmark-stress` exposes a 5M-snapshot tier with the same safety guard; it was
**not run**. The configured reserve is a VM-disk check, not a full host-memory safety
guarantee. Cancel if host pressure becomes unsafe. No final benchmark result is
inferred for this planned tier.

## Correctness and interpretation

Before scale: pinned corpus verification, deterministic mutation/diff tests, native
Spark transformation tests and the small public-fixture → Kafka → committed MinIO →
analytics integration run. The controlled six-asset rollout produces 36 snapshots,
30 adjacent diffs, 12 logging-rule alerts and one detected hourly fleet burst at
2026-01-03 00:00 UTC. Repeating analytics reuses the same committed result/run ID.
That is a controlled ground-truth check, not detection accuracy on real incidents.

Always separate observed results, arithmetic projections, and planned/not-run work.
Record unsuccessful overload/timeouts, not just the best point. Correlated reuse of
324 small baselines affects compression/entropy; it cannot establish production file
sizes or storage ratios. Statistical anomalies are explainable historical thresholds
using prior windows, not ML or forecasting. See [analytics semantics](historical-analytics.md).

Safe report claims: reproducible public-fixture integration, version-adjacent state
correctness, committed historical analytics, measured local replay/storage behavior.
Unsafe claims: naturally thousands of changes/s per router, synthetic volume proving
real Big Data, production latency/throughput, distributed cluster scalability or
end-to-end exactly-once. No distributed transaction spans MinIO/Kafka/search.

Planned/not-run unless a result file explicitly proves otherwise: 5M+ stress, production
fleet observation, multi-node scaling, partition-count experiments and final deployment.

## Observed results — 2026-10-11

All 21 recorded executions are retained: **17 completed, 4 failed**. The following
representative rate/scale table is not the complete run inventory; the CSV and
per-run JSON include correctness, duplicate, late-data, local[4] and Makefile
executions too. GB means decimal bytes / 1,000,000,000.

| Run | Status | Assets | Input / accepted snapshots | Logical config GB | Target snapshots/s | Producer snapshots/s | Spark reported rows/s | Streaming seconds | Committed Parquet GB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| final-smoke-100-r1 | success | 500 | 10000 / 10000 | 0.012091 | 100 | 100.01 | 296.35 | 111.77 | 0.012620 |
| final-smoke-500-r1 | success | 500 | 10000 / 10000 | 0.012091 | 500 | 499.99 | 1295.15 | 33.48 | 0.010686 |
| final-smoke-500-r2 | success | 500 | 10000 / 10000 | 0.012091 | 500 | 500.00 | 1066.21 | 33.90 | 0.010719 |
| final-smoke-500-r3 | success | 500 | 10000 / 10000 | 0.012091 | 500 | 500.02 | 1080.83 | 33.86 | 0.010741 |
| final-smoke-1000-r1 | success | 500 | 10000 / 10000 | 0.012091 | 1000 | 999.98 | 1058.21 | 24.56 | 0.010575 |
| main-100k-20261011 | success | 1000 | 100000 / 100000 | 0.112995 | 1000 | 1000.00 | 2549.65 | 119.06 | 0.104548 |
| main-1m-20261011 | failed | 10000 | 600358 / 200110 | 0.686958 | 1000 | n/a | 1082.78 | n/a | 0.161766 |
| main-1m-2g-50k-20261011 | success | 10000 | 1000000 / 1000000 | 1.144676 | 1000 | 1000.00 | 2678.37 | 1065.62 | 0.876030 |

Failed publisher input counts mean attempted sends where delivery flush did not
complete, not confirmed acknowledgements. The old failed million attempt has no
recorded streaming-wall metric; total application wall time was 606.56 s. Native
Spark rows/s is the arithmetic mean metric with three source branches, **not**
logical configuration throughput.

The final 500/s, local[2], 10k series has three repetitions: median streaming wall
**33.857 s** (range 33.478–33.899 s), median logical accepted rate **295.357/s**.
The final 100/s and 1000/s runs complete at 89.472 and 407.248 logical snapshots/s,
including setup within streaming/drain. They do not establish steady-state capacity.
An earlier local[4] smoke finished in 28.585 s versus 33.785–33.835 s for its earlier
local[2] counterparts: one local[4] repetition with the earlier policy, not cluster
scaling. The default Makefile smoke later also passed, with Elasticsearch running
idle for regression acceptance; it is not part of the original controlled series.

### Main scale run

The successful 10,000-asset × 100-version run accepted **1,000,000 snapshots**,
**990,000 adjacent diffs**, **250,000 rule alerts**, and **680,000 state_eviction
diagnostics**. Metadata joins found zero missing, cross-asset or nonadjacent diff
pairs; persisted frequency sums agree. All 324 source paths (268 distinct content
hashes) were sampled with replacement, not 10,000 observed devices. The workload
covers 100 simulated UTC calendar dates, January 1–April 10.

Input content: **1,144,675,896 bytes**; serialized snapshot values:
**1,622,093,126 bytes**. The 25 committed batches store **876,030,173 Parquet bytes**
across raw and processed zones, not a raw-only compression ratio. Producer time:
1000.001 s; streaming/drain: 1065.618 s, including 65.618 s drain. Logical accepted
throughput: **938.422 snapshots/s**. Final lag: zero; peak sampled
latest-minus-completed-offset lag: **99,593** records.

Active micro-batch p50/p95: **48,026 / 49,932 ms**. Maximum exposed operator rows:
**89,997**, memoryUsedBytes: **17,510,807**. RocksDB/operator metrics do not measure
all retained content or process RAM. Java peak sampled RSS: **2,487,205,888 bytes**;
peak ps CPU: **417.4%**. Kafka peak sampled Docker memory/CPU:
**1,017,223,577 bytes / 147.2%**; MinIO: **487,797,555 bytes / 37.81%**.
These are individual sampled peaks, not full-pipeline totals.

Machine: macOS 27.0 arm64, Apple M3 Pro, 12 host cores, **19,327,352,832 RAM bytes**;
Docker VM reports 2 CPUs and 6,189,654,016 RAM bytes. Python 3.12.14, Java 21.0.12.1
(Homebrew), Spark 4.0.2, Kafka 4.1.2, Docker 29.8.1. Physical storage hardware was not
probed. One machine with local Spark, not a distributed cluster. Main: **one repetition**.

### Historical batch measurements

Eight persisted jobs ran after streaming stopped, in the warm local session.
Summed per-job count/write timers: **8.134 s at 10k** (final 1000/s run),
**27.041 s at 100k**, **85.121 s at 1M**. Main per-job timings:

| Job | Seconds |
| --- | ---: |
| change-frequency | 14.427 |
| fleet-activity | 13.285 |
| diff-magnitude | 5.339 |
| alert-trends | 1.813 |
| diagnostic-trends | 4.807 |
| asset-summary | 11.108 |
| anomalies | 17.690 |
| fleet-bursts | 16.653 |

These exclude session startup/input-plan construction and are not cold-read application
timings. Asset/version shapes and file counts vary; logical publisher input bytes are
a volume proxy, not bytes actually scanned by metadata-only analytics. There is no
isolated volume-only scaling or statistical significance claim.

### Failures and bounded-history experiment

- Initial correctness execution: progress parsing failed with JSONDecodeError.
  Spark 4 progress now uses its native JSON property. Later correctness/regression
  tests pass; the failed record remains.
- First million target, 1g heap / 10k offsets: underlying Java heap OutOfMemoryError
  during Parquet writing. It attempted 600,358 snapshots and committed 200,110;
  committed Parquet: 161,766,495 bytes. Rerun used **2g heap and 50k offsets**.
  Both knobs changed, so no isolated causal attribution is claimed. This is the
  first observed hard limit, not the machine maximum.
- Controlled 1-second watermark, 300-record reordering, 100-offset triggers:
  1,000 input, **423 watermark drops**, **577 accepted**, **277 diffs**.
  The completeness guard deliberately marks it failed. All 1,000 raw records are
  committed; losses are not hidden as successful processing.
- Small fleet, 100 assets × 100 versions, cap 32, 10k offsets: 9,729 accepted of
  10,000; 9,470 diffs. Diagnostics: 6,529 state_eviction, **271 evicted_version**,
  159 unresolved_version_gap. This is grouped-iteration/cap pressure, not watermark
  dropping (reported zero). The default 500 × 20 smoke subsequently passed.
  No Phase 6 redesign or weakened completeness assertion was applied.

[Independent metadata/analytics checks](../benchmarks/results/correctness-verification.json)
also cover the full-corpus rollout: 600 snapshots, 500 diffs, 200 alerts; one detected
hourly fleet burst at 2026-01-03T00:00:00Z. Its DNS campaign has 100 assets/diffs and
zero alerts; alerts occur at logging changes in other windows. Hourly minimum count
10 excludes ordinary 4–5-asset maintenance windows. This is a controlled evaluation,
not incident-detection accuracy. A 150-record reordered delivery experiment accepted
1,000 unique snapshots and 950 adjacent diffs, dropped 25 exact duplicates, and had
339 deliveries below an already seen asset version (including duplicate deliveries).

No 5M+ run or rate above 1000/s was executed. Backlog forms and drains on completed
runs. Configured JVM heap and bounded history are observed limits; detailed CPU,
storage and operator bottleneck attribution requires separate experiments. Parquet
bytes and warm batch times increase with measured tiers, but compression, batching,
corpus reuse and workload shape confound extrapolation.
