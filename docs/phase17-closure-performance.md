# Phase 17.1 — measured rescanning and bounded reuse

## Evidence and profiling boundaries

The earlier twelve callback timings are not an asymptotic proof. This experiment
profiles the actual Phase 17 functions and captures Spark task input/shuffle/spill
counters. It records inclusive wall time and call counts for `committed_rows`,
`reconcile_rows`, `projection_digest`, `state_digest`, publication and finalization.
Nested function times overlap and must not be summed. Task bytes are Spark
filesystem metrics, not physical device reads or Kafka/S3 network traffic.

Selection: the pinned September 1 archive, **8,192 accepted rows**, 32 source
chunks, eight 1,024-row **local** callbacks. Each run uses fresh JVM, query UUID,
output directory, `local[2]`, 2 GiB driver heap and eight shuffle partitions.
Existing Kafka/MinIO services were idle during all six matched runs. OS file cache
was not evicted or controlled. This is a partial-day pilot: **zero completed days**.
It is not service throughput, natural arrival velocity or a fourteen-day benchmark.

## Matched results

| Mode | Individual callback-total seconds | Median / range | Earlier-segment checksum visits | Declared rows visited for these checks |
|---|---|---|---:|---:|
| Baseline | 31.657, 30.533, 31.143 | 31.143 / 30.533–31.657 | 64 | 65,536 |
| Callback reuse | 22.517, 22.564, 22.454 | 22.517 / 22.454–22.564 | 28 | 28,672 |

Median reduction: **27.70%**, descriptive only (three repetitions, no significance
or general scalability claim). All six runs passed the exact independent partial
source-map anti-joins and committed-ledger validation. Sorted raw batch digests,
row counts and observation IDs were identical across modes; physical file paths
and UUIDs differ. All slower and exploratory runs remain reported in
[before/after results](../workloads/results/phase17-closure/before-after-performance.json).

Whole-run Spark task input counters, including final readback validation, were
311,954,656 bytes for each baseline and 218,868,472 for each reuse run. These are
not compressed-source volume or saved network bytes. Peak sampled process-tree
RSS ranged from 1,556,742,144–1,663,959,040 baseline and
1,441,726,464–1,556,283,392 reuse bytes. Sampling can miss peaks and double-count
shared pages. No memory/disk spill or resource guard failure was observed in this
pilot; it does not prove constant memory for arbitrary input.

## Root cause and limits

Baseline reads/verifies earlier active-day raw segments twice per callback. For
`n` equal-size partial callbacks the measured code path performs
`sum((i-1)+i) = n²` segment checksum visits; here 64. Reuse performs the first
read only, `n(n-1)/2`, here 28, and **adds a full physical reread at completion**.
This analytical statement concerns this segment-visit schedule, not a claim that
total runtime is exactly quadratic: oracle scans, sorting, scheduling, file sizes,
shuffle, digest iteration and publication contribute other costs.

The smallest safe change is described in
[controlled amendment](phase17-closure-amendment.md). It does not introduce a
persistent index, change old manifests, trust counts instead of identities, or
deduplicate by membership payload. Accumulated active-day reconciliation remains;
full source-map and canonical-set checks still guard completion. Chunk size and
micro-batch count therefore remain important. Daily scoped ingestion prevents
unnecessarily mixing all fourteen states into every callback, but arbitrary
multi-day disorder still needs correctness tests and may cost more.

The serial, globally sorted digest iterator remains a bottleneck. Source-map
oracle scans and repeated anti-joins remain. No distributed digest algorithm,
generic workflow engine, compaction system or horizontal-scaling result is claimed.

## Negative evidence and fair comparisons

An exploratory profile correctly validated its partial rows but failed report
assembly by looking for an absent `observation_id` summary field. It is labeled
`FAIL_REPORTING`, not accepted performance evidence. After fixing the report,
another exploratory baseline took 32.434 seconds. Neither exploratory run is
silently substituted into the three matched pairs.

The old fourteen-day SQLite oracle pass and two-day Spark timings have different
projections, caching and work. They are not a fair engine comparison and remain
unchanged. RQ2/RQ3 matched batch-versus-stream/query experiments remain Phase 20
work. Real service-stage parsing, broker-acknowledged JSON value bytes, processing,
reconstruction, committed bytes and resource costs are reported separately in
[closure integration](phase17-closure-integration.md).

## Reproduction

With the existing locked Spark environment and verified private archive/oracle:

```sh
.venv/bin/python -m benchmarks.rpki_closure --mode baseline --output .cache/phase17-closure/new-baseline
.venv/bin/python -m benchmarks.rpki_closure --mode reuse --output .cache/phase17-closure/new-reuse
```

Use new directories for each repetition and alternate mode order. Normal CI uses
owned tests for report aggregation and event-counter parsing, not these archives.
Raw Spark event logs stay private because plans/records can contain source data.
