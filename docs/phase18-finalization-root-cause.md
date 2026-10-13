# Phase 18.2 — RIS finalization root cause

Starting `main`: `f5b62bb947e6dfa4626687f08ff2245b928b34b1`, clean;
its [CI passed](https://github.com/kongwoang/ConfigStream/actions/runs/38384548567).
The previous [acceptance](phase18-closure-acceptance.md) remains immutable.

## Two different attempts

| Attempt | Evidence | Interpretation |
|---|---|---|
| `5f937f78-ac0d-4b4c-a5f6-242cd734852f` | Owner requested sleep; SIGINT, Py4J interruption, shutdown StackOverflowError; 41 commits | Interrupted ingestion; no complete window |
| `24ef7626-8ac9-4978-b2e8-6e6bfc0bd6c8` | 151 commits, all twelve archives acknowledged; stage 4841 task 0 failed allocating aggregation memory | Failed global finalization; no complete window |

The second preserved driver log records `RowBasedKeyValueBatch: Failed to
allocate page (33554416 bytes)` at local time 2026-10-13 16:08:27, followed by
`SparkOutOfMemoryError: No enough memory for aggregation`. The generated iterator
has three nested `hashAgg_doAggregateWithKeys` frames before the shuffle writer.
The Python stack identifies `exact_projection(..., complete=True)` at its
identity-conflict count, not the earlier bounded SHA iteration. Task 1 was
cancelled after task 0 failed. No SIGINT, InterruptedException or StackOverflowError
appears in this second driver log.

Filesystem UTC timestamps place source progress publication at 09:00:59,
checkpoint batch 150 acknowledgment at 09:04:40 and final failure result at
09:08:28. These are file publication timestamps, not complete task timers.
The preserved runtime directory contains no Spark event log or captured SQL plan;
original task spill/shuffle/peak execution-memory counters cannot be recovered.

Read-only macOS power logs do show sleep/wake transitions during the second run,
including a wake at 16:08:15 before the failure. Their existence is established;
their causal contribution to aggregation allocation failure is not. Sleep may
affect elapsed-clock comparison. It does not establish that the shutdown failure
from the first attempt caused the second failure. Previous measurements remain
reported with both clock definitions.

## Falsifiable diagnosis

The observed failure is execution-memory allocation in a high-cardinality hash
aggregation. The code performs full-row deduplication, logical-ID aggregation and
projection set differences, then repeats these operations on a cached logical
projection at finalization. Two shuffle partitions and AQE disabled are verified
configuration facts. A small reconstruction of the old expression contains four
HashAggregate nodes; it is **not the original stage 4841 plan**.

The relative effects of caching, skew, union-plan growth and partition size were
not measured in the failed run. No asymptotic complexity or sleep-causation claim
follows. The proposed correction is refuted if it misses a conflicting/missing/extra
identity, changes the checksum, or cannot complete within the registered heap and
runtime. Full-window recovery must establish that separately from small fixtures.

Spark's [EXPLAIN documentation](https://spark.apache.org/docs/4.0.2/sql-ref-syntax-qry-explain.html)
defines plan inspection; its [memory tuning guide](https://spark.apache.org/docs/4.0.2/tuning.html)
describes execution/storage memory sharing. Actual observations and reconstructed
plans are recorded separately in the [experiment report](phase18-finalization-experiments.md).

## Separate ordered-cache defect found during recovery

The first corrected recovery passed global comparison and all twelve source-order
checks, then failed at the global digest's maximum-partition barrier. Stage 1526
shows `Java heap space` in `MemoryStore.SerializedValuesHolder` / serialized block
allocation. This is direct evidence of a storage-cache allocation failure, rather
than a recurrence of stage 4841's occurrence-key hash aggregation. Move the ordered
cache from MEMORY_AND_DISK to DISK_ONLY; keep the exact sorting, row-size guard
and SHA stream. The reviewed same-cap recovery then completes in the same heap.
That successful execution supports the targeted correction, not an asymptotic
scalability claim or a general guarantee that all future inputs fit 2 GiB.
