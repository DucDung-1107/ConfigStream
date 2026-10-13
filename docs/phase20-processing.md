# Phase 20 — processing costs and pending matched engines

## Corrected, separately approved pilot: 27 exact trials

[All repetitions and paired ratios](../workloads/results/phase20/matched-processing-repetitions.json)
bind execution `a79c21c8f37e823e29d6c40949cfc3c397804f01`: all nine configurations
have three correct fresh-process repetitions. Every full normalized row matches
the accepted independent projection; no missing/extra/conflicting identity.
All9 B3 trials end at zero selected-offset lag. Parsing and production state/group
finalization are excluded in every mode; queued availableNow is not paced H3.

| Selected unit | B0 wall median (range), s | B2, s | B3, s |
|---|---:|---:|---:|
| RPKI360,226 canonical members | 6.297 (6.282–12.577) | 16.840 (14.729–21.080) | 25.417 (23.144–25.424) |
| RIS370,141 native occurrences | 10.485 (8.437–12.636) | 21.167 (18.967–23.186) | 29.644 (23.233–29.726) |
| DNS4096 raw measurements | 2.089 (2.087–6.302) | 10.536 (10.486–12.584) | 16.866 (12.624–16.924) |

Command-start to observed-exit includes startup, publication, exact verification
and shutdown, with up to2s polling resolution. B0 was faster for these registered
normalized core tasks, not universally. Spark repartitions to two writers, Arrow
streams to two staging files before upload; plans, row-group layouts and retained
scratch differ. Do not rank optimized systems or physical representations from
these component bytes. Filesystem cache was uncontrolled, not proven cold.

Retry427.655s plus initial75.327s = **502.981s**, below aggregate7,200s.
Retained host content plus guest free-space decrease, including the failed attempt:
**3,869,485,576bytes**, below amended6GiB (above original3GiB). Peak sampled tree
**1,612,824,576bytes**; guest RAM minimum4,191,440,896bytes; final guest disk free
30,334,918,656bytes. VM/heap unchanged, no resource failures.
All27 trial Parquet1,622,766,994bytes; MinIO namespaces1,623,023,143bytes;
selected9Kafka directories558,968,199 apparent/181,870,592 allocated bytes;
checkpoints9,393 content bytes. Backend allocation/network I/O are unmeasured.

Three-repeat B3 ACK totals by role: RPKI1,080,678 canonical rows/8,445chunks;
RIS1,110,423 native occurrences/8,676chunks; DNS12,288 raw rows/96chunks.
Never pool these as one native event rate. Per-trial startup, producer/value-byte,
Spark chunk-scan and validation timings remain recorded. CPU/task shuffle/spill,
heap occupancy, physical query I/O and lag time series are NOT_MEASURED.
No new fault-injection campaign or old-checkpoint recovery is claimed. Initial
3PASS/1FAIL is retained separately, not averaged into corrected medians.

## Approved matched core campaign: stopped, not silently retried

The [specific registration](../workloads/results/phase20/matched-pilot-registration.json)
received owner approval for 27 trials, aggregate 7,200s/3GiB, unchanged VM/heap.
Execution commit `138f4fc6dfeceb8dd6855e1614c9612144d79db5`:
**three PASS, one failed, 23 NOT_RUN**; campaign stopped after **75.327s**.
[Every attempt](../workloads/results/phase20/matched-processing-attempt-1.json)
retains input bindings, exact projections, timings, isolation and resources.

| Trial | Command start to observed exit | Exact result |
|---|---:|---|
| DNS4096 B2, repetition2 | 14.732s | Full4096 source rows match |
| RIS5min B0, repetition2 | 12.622s | All370,141 event rows match |
| RPKI1day B0, repetition2 | 6.267s | All360,226 canonical members match |
| DNS4096 B3, repetition3 | Failed | 32 chunks/4096 rows acknowledged; no accepted commit |

These are **INSUFFICIENT_REPETITIONS**, and no workload has all matched engines;
do not rank these three cross-domain times or claim a streaming speedup.
Source parsing and production state/group finalization are excluded in **every**
mode; all retain the same full normalized row in a canonical JSON wrapper.
Snappy/two partitions match, but row-group layouts differ. Spark write timing
includes S3A; B0 local-write and upload timing are separate. Only all-in boundaries
would support matched core comparison. Parent exit observation has up to2s
polling resolution; fresh process does not prove cold filesystem cache.

The fourth trial failed because SQLite connections created in the main Python
thread were used by Spark's foreachBatch callback thread. This is a **new benchmark
harness defect**, not the earlier RIS aggregation OOM or a changed source oracle.
Read-only MinIO audit found two uncommitted Parquet files, **zero batch commits**,
and **no completion**. Failed files/topic/checkpoint are retained, not accepted.
[Repair record](../workloads/results/phase20/matched-pilot-repair.json) describes
serialized cross-thread connection use and owned thread/Spark callback tests.
The owner subsequently approved exactly one corrected retry. Its
[controlled amendment](../workloads/results/phase20/matched-pilot-retry-registration.json)
keeps aggregate7,200s including75.327s already consumed, raises aggregate retained
growth to6GiB including failed evidence, and leaves VM/heap/reserves unchanged.
Retained normalized inputs are reused read-only after checksum/full-ledger audit.
This is not authorization for full-hour replay or a third attempt.

Peak sampled campaign process tree:986,693,632bytes; minimum guest available
RAM4,304,404,480bytes; guest free disk32,142,180,352bytes. Combined new host content
plus guest free-space decrease:1,051,973,682bytes, within3GiB. No guard/OOM failure.
Measured JSON-wrapper output and reference overhead make completing all27
retained trials under3GiB questionable; the separate **owner-approved** retry
budget is recorded. No runtime-cap increase, automatic retry or full-hour replay.

[Registered cells](../workloads/results/phase20/evaluation-matrix.json) and
[pilot trials](../workloads/results/phase20/pilot-results.json) distinguish
normalized direct-Parquet work from raw parsing and real transport.

Three trials each re-encode one complete RPKI day (360,226 members), first complete
RIS five-minute accepted projection (370,141 occurrences) and first 4096 DNS raw
measurements. Every full Arrow projection compares exactly. Same Snappy/65536-row
target; no native events or statuses are discarded. Startup/module-import time
is NOT_MEASURED; worker elapsed begins after imports. Parser construction is
EXCLUDED. No Kafka messages or Spark callbacks were produced by this pilot.

Thus B0/B2/B3 full-window matched engine comparisons are **NOT_RUN**. The corrected
27 core trials complete only the bounded matched pairs above. Existing
RPKI 5,053.741-s full service run and DNS 1,224.086-s run include different parsing,
publication, verification and recovery work. Do not compare the pilot's direct
rewrite time or oracle time against them as an engine ranking. The prior RIS
full-hour ingestion failed at finalization and was later recovered; it is not a
new successful clean ingestion benchmark.

H3 is **NOT_RUN**: no 0.5/1/2x paced replay was started. Required 1x rule remains
exact logical IDs, zero selected-offset lag, resource compliance and
post-producer-to-final-validated-commit drain <=10% of producer duration.
Accepted rate includes stream-ready through final validated commit. Do not hide
validation time or substitute historical 1,312.543333 source occurrences/s.

[Budget proposal](../workloads/results/phase20/budget-proposal.json) uses ideal
uniform target arithmetic: full nested 3-rate/2-pacing/3-repeat producer work alone
is approximately **28.44 hours**, excluding startup/drain/validation; burst pacing
may differ. Old raw-parsing times are labeled unmatched estimates, not completion
promises. Current guest headroom above reserve is about 10 GiB; complete retained
matrix growth is not established/approved. No VM resize, deletes or full-hour
replay. The initial bounded service pilot approval was consumed by the stopped
campaign; its single explicitly approved retry passed. Further/full rate runs
require a new decision; neither consumed authorization permits another run.

All per-run measurements, slower runs and the initial virtualenv bootstrap failure
remain recorded. The failed namespace is not renamed successful or averaged.
CPU, physical query I/O and network wire bytes are NOT_MEASURED, not zero.

```sh
source .cache/local-kafka-env.sh
caffeinate -i .venv/bin/python -m benchmarks.phase20 --suite pilot --output .cache/phase20/pilot-new
```
This command is normalized local-only, not authorization for E3 service expansion.
