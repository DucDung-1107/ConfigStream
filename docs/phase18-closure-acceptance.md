# Phase 18.1 — acceptance and remaining gates

## Decision

**CONDITIONAL PASS — FULL SERVICE OR ORACLE QUALIFICATION REMAINS**

The independent full-hour accepted projection is established, and meaningful
three-archive broker/streaming/physical-readback work completed. The first
fifteen-minute attempt failed with Java heap exhaustion; the explicitly approved
same-cap retry **passed complete-window publication and exact query validation**
after bounded digest iteration. The largest fully accepted service window is now
three complete archives / fifteen source minutes. No whole-hour
service run is authorized or claimed; Phase 19/20 has not started.

Starting clean `main`: `64a29734e84c37561555fad5ddb0cf6951414557`.
Implementation corrections end at `7d2c90e2bf2dc1de456d4bb584e51d8aded4ada8`;
the final documentation/evidence delivery SHA and exact CI run are reported in
the agent handoff. Original Phase 13–18 evidence remains unchanged.
Implementation CI at that exact SHA
[passed](https://github.com/kongwoang/ConfigStream/actions/runs/38348594755);
it ran owned/service-free tests, not the real reference window.

## Stage acceptance

| Stage | Result | Actual evidence |
|---|---|---|
| A: owned fixtures | PASS_INTEGRATION | Independent attributes/order, corruption, replay, sidecars; 14 actual-service tests passed |
| B: first five-minute file | PASS_SELECTED_WINDOW | 370,141 exact events, 5 durable exclusions, complete-window metadata and query agreement |
| C: first three files | PASS_SELECTED_WINDOW | Same-cap corrected retry: 1,303,847 exact events, lag zero, 14 durable exclusions, complete-window descriptor and queries |
| D: twelve-file oracle | PASS_REAL_WINDOW, level 3 | 4,725,156 exact accepted identities/projections; all required wire attributes independently checked |
| E: full-hour service | BLOCKED | Separate runtime approval requested after successful pilot, not yet assumed |

The source result matches 2,300,578 frames, 2,291,545 UPDATE messages, 57 excluded
body records, 4,462,391 announcements and 262,765 withdrawals. The fingerprint
union has 4,606,984 keys / 118,172 repeats beyond first; zero additional cross-file
repeats under the timestamp-bearing policy is calculated by exact SQL, not assumed.
Per-archive counts, source hashes and digests are in the
[oracle report](phase18-closure-oracle.md) and [machine ledger](../workloads/results/phase18-closure/occurrence-ledger-validation.json).

## Implemented changes and limits

- `workloads/ris_reference.py`: independent limited raw-wire attributes and complete
  accepted occurrence projection, preserving frozen JSON/fingerprint serialization.
- `workloads/ris_oracle.py`: bounded Parquet/SQLite exact ledgers, 12-file union,
  explicit unknown exclusion evidence and verified immutable-segment reuse.
- `schemas/ris.py`, `lake/ris.py`: strict immutable source/exclusion/accounting/window
  descriptors and fail-closed committed readers; old physical `ris-1.0/1.1` meanings
  remain unchanged.
- `spark/ris.py`: exact replay/conflict validation, sealed-window retry suppression,
  optional scoped-oracle parity, callback timers and bounded ordered digest iteration.
- `benchmarks/ris_service.py`: explicit approval/cap guard, canonical Spark offset
  JSON, real broker lag, exact source finalization and measurements including failures.

No universal state processor, routing table, incident classifier, DNS pipeline,
new API or configuration/RPKI migration was added. The parser still defines the
frozen acceptance/exclusion disposition. Independent attribute values do not turn
that policy into a universal BGP validity oracle. Opaque attribute families retain
their declared representation without invented semantics.

## Services, recovery and negative results

Existing Kafka 4.1.2 / MinIO and Lima 2-CPU/6-GiB/64-GiB environment were reused.
Host Spark 4.0.2 stays `local[2]` with a 2-GiB heap. No VM expansion, downloads,
volume deletion, paid services or raw external Git publication occurred.
Services and the VM were stopped after initial validation and restarted for the
owner-approved retry; all volumes/evidence remain. Final state is reported at handoff.

Fourteen distinct real-service owned-fixture tests pass after the digest correction:
four callback failure points, invalid transport, eight missing/corrupt metadata/
Parquet/sidecar cases and a cross-archive/checkpoint/window/sealed-retry case.
This is a single-writer callback failure model, **not** power-loss, broker failover
or a global transaction. Replay retains distinct source positions even with equal
fingerprints; sealed retries do not alter the fixed completion inventory.

Preserved failed attempts: 13 authentication-prerequisite fixture failures; missing
independent type-21 attribute coverage in the first whole-source oracle attempt;
five-minute offset-reporting JSON failure; fifteen-minute Java-heap failure. They
are not counted as successful experiments. The latter left 42 committed batches,
168 Parquet files / 247,864,779 bytes and 14 durable exclusions, but no window
completion. Its 1,007.260-second execution and 2,873,704,448-byte sampled RSS peak
are negative capacity evidence, not completed-window performance.

The bounded digest correction preserves exact ordered SHA bytes and passes
independently specified ordering/skew tests. Owner-authorized retry at the same
1,800-second cap passed in **1,056.353 seconds**: 42 commits, 168 Parquet files /
247,864,779 bytes, 14 source exclusions and zero missing/unexpected identities.
The complete projection SHA is
`a2a0910bd20441aaf9a12c863c558b5f2b4c05198e932b95d49199fd79a151b3`.
Final readback/query validation took 103.618 seconds. Peak sampled RSS is
2,740,961,280 bytes, minimum guest free disk 41,690,152,960 bytes and available
guest RAM 4,437,966,848 bytes. No cap or sampled guard was breached.
The failed attempt remains preserved; a failed-to-completed comparison is **not**
a matched speedup experiment. See [three-file acceptance](../workloads/results/phase18-closure/three-archive-validation.json).

## Invariants

| Invariant | Evidence status | Scope / unresolved limit |
|---|---|---|
| I1 provenance | PASS_INTEGRATION | Twelve pinned sources; owned corrupt references; no full-hour service inventory |
| I2 replay | PASS_INTEGRATION | Owned actual broker/checkpoint/sealed retries, not whole-hour replay |
| I3 multiplicity | PASS_REAL_WINDOW | Exact 12-source union plus cross-archive service fixtures |
| I4 completeness | QUALIFIED | Fifteen-minute complete-window accepted; full-hour completion pending |
| I5 temporal honesty | PASS_REAL_WINDOW | Exact historical timestamp projection, no causal/routing-state claim |
| I6 committed visibility | PASS_INTEGRATION | Actual isolated corruption and partial-write rejection |
| I7 reproduction | QUALIFIED | Twelve-source level 3; accepted fifteen-minute service level 4; full hour pending |
| I8 error accounting | PASS_INTEGRATION | All 57 audited; 5 accepted-window / 14 larger-attempt durable exclusions |
| I9 compatibility | PASS_LOCAL | Existing Terraform/RPKI regression and paths unchanged |

Statuses are scoped; no constructor or owned fixture proves all-hour service
recovery. See [invariant evidence](../workloads/results/phase18-closure/invariant-coverage.json).

## Validation and scientific limits

Local `make check`: **1,023 passed**; `make api-test`: **80 passed**;
`make spark-test`: **80 passed**. Relative to Phase 18: +39 unit and +7 Spark
tests; API unchanged. Latest complete actual-service fixture suite: **14 passed
in 127.95 seconds**, separately from standard CI. Normal CI needs neither live
RIPE downloads nor Kafka/MinIO. One existing API dependency warning remains.

RQ1 now has independent native-event/source and scoped recovery evidence. RQ2/RQ3
remain preliminary storage/timing evidence, including a negative larger-window
capacity result. No matched batch/streaming speed comparison, paced steady-state
replay, physical task I/O/shuffle/spill profile or asymptotic claim is established.
Source mean 1,312.543333/s and peak 181,868/minute are not pipeline throughput.

Raw archives/complete normalized ledgers stay private. RIPE NCC/RIS attribution
is retained; unrestricted external-data redistribution and instructor approval
are not verified. Research-use review is not a blanket raw-publication license.

## Remaining gates and rollback

1. Retain the failed fifteen-minute attempt and the accepted same-cap retry.
2. Obtain explicit approval of the proposed 7,200-second whole-hour cap, based on
   the successful 1,056-second pilot; no VM/heap or disk-guard increase is proposed.
3. Only then run twelve files and verify all logical events, 57 durable exclusions,
   complete-window metadata, queries, resource use and selected broker lag.
4. Obtain instructor/publication approval where required before broader academic
   or external-data release claims. Do not automatically start DNS or Phase 20.

If bounded digest iteration still fails, stop and preserve evidence: reconsider
physical partitioning/finalization within the same event projection rather than
silently increasing heap, discarding events or weakening the oracle. Rollback
disables the new RIS experiment/metadata path; no old lake migration or data
deletion is required.

Related: [integrity](phase18-closure-integrity.md),
[experiments](phase18-closure-experiments.md),
[service measurements](../workloads/results/phase18-closure/service-validation.json),
[recovery tests](../workloads/results/phase18-closure/checkpoint-recovery-validation.json).
