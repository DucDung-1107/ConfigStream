# Phase 17.1 — RPKI end-to-end acceptance closed

**PASS — PHASE 17 FULL END-TO-END ACCEPTANCE CLOSED.**

The exact pinned fourteen-day RPKI window passed through real Kafka → Spark
Structured Streaming → MinIO. All daily states, thirteen boundaries and full/delta
reconstructions match the independent source oracle. Eight actual-service owned
checkpoint fault cases and one invalid-transport case passed. This closes the
selected RPKI engineering acceptance, **not** RQ2/RQ3, instructor approval, global
RPKI coverage or the four-dataset research. Phase 18 was not started.

## Revision and historical evidence

Starting clean `main`: `72705d6bd77e65e9aa3b6bf24c97063d5b03ae84`.
The previous [Phase 17 partial decision](phase17-acceptance.md) and all Phase
10–17 measurements remain unchanged. Implementation commits:

- `7722df0b5e9619394f0c03c1a070dd769f25ad39`: bounded callback reuse, profiling,
  service driver and targeted regression/recovery tests.
- `a70d557e669a8066b95c5f6536423c69a5d2127c`: preregistered full-window cap,
  execution fingerprints and fail-closed missing-RSS monitoring.
- `50bf7ae30c5ea4e7f759e8b52ee059509236bacb`: independent SQL transition-ID audit.

The full run started at the first revision with the approved driver changes.
[Daily validation](../workloads/results/phase17-closure/daily-pipeline-validation.json)
pins the actual execution SHA-256 for the driver and source/processing/oracle
modules. The later RSS-error hardening was not loaded inside this already-running
process; every recorded RSS sample was independently checked available. See the
[controlled amendment](phase17-closure-amendment.md). Final delivery-commit CI is
verified separately in the handoff; this document does not fabricate a future run.

## Stage acceptance and exact source evidence

Source: RIPE RPKI archive, `ripencc.tal`, September 1–14, 2026. All fourteen cached
XZ SHA-256/size checks were repeated successfully against the original
[source manifest](../workloads/results/phase17/source-manifest.json). No new source
download occurred. Compressed source: **63,816,988 bytes**; logical CSV:
**909,372,097 bytes**. The independent SQLite oracle was reused unchanged/read-only.

| Stage | Executed evidence | Result |
|---|---|---|
| A | 3 matched baseline/reuse local pilot pairs; exact same 8,192-row projection | PASS |
| B | Actual Kafka/MinIO owned fixtures; 8 checkpoint fault points + invalid transport | 9 passed, 176.49 s |
| C | 1 complete real day; 360,226 exact source rows/members and reconstruction | PASS, 298.913 s |
| D | 2 real days; 720,732 rows; 717 additions, 437 removals | PASS, 595.527 s |
| E | 14 real days; 5,062,923 rows; 13 exact boundaries | PASS, 5,053.741 s |

C/D/E windows overlap: **do not sum them as new independent source observations**.
The full run took **84.229 minutes**, below the **120-minute cap approved before
its first attempt**. There was no failed full-window attempt or timeout increase.
All 14 days completed, with **0 source parser exclusions**, **7,096 additions**,
**4,920 removals**, **12,016 transitions**, and zero unexplained missing/extra
logical identities. The independent union is 367,204 memberships; exact per-day
agreement implies equal union, not a separate measured Spark global-union query.

Every day passed exact source-coordinate/member-set anti-joins, scope/artifact/
policy binding, count accounting and sorted canonical/source-map digests. Every
boundary passed an additional **complete physical transition-ID set comparison**
against independently hashed read-only SQLite `EXCEPT` results. Full-state reads
and baseline-plus-transition reconstructions match every accepted daily set.
Exact per-day/transition hashes and observation identities are in:

- [Daily pipeline ledger](../workloads/results/phase17-closure/daily-pipeline-validation.json).
- [Transition identity ledger](../workloads/results/phase17-closure/transition-pipeline-validation.json).
- [Reconstruction ledger](../workloads/results/phase17-closure/reconstruction-validation.json).

## Broker, object store and recovery

The full run uses topic
`temporal.rpki.fixture.44d52ae9-a2e6-4f88-bf48-d77107f85df0`, prefix
`phase17-closure/44d52ae9-a2e6-4f88-bf48-d77107f85df0`, and a fresh isolated
checkpoint. The `fixture` namespace is an allowed isolation name; these are
**real** reference data, not generated observations.

The broker retained **19,785 chunks**, two partitions, seven-day delete retention,
and matching final Spark offsets **9,886 / 9,899**: final lag **0 chunks**. Producer
ACK-bound JSON value bytes were **944,206,867**, not measured wire bytes.
Kafka's reported selected partition log sizes total **946,917,404 bytes**.

Actual MinIO conditional publication passed identical retries and conflicting
content rejection. Partial/uncommitted files were invisible to committed readers;
absent completion rejected reconstruction. Actual Kafka checkpoints recovered at
all eight declared owned callback interruption points. Equal source redelivery
remained one logical occurrence; distinct source coordinates/dates survived.
The invalid broker record failed closed with quarantine and no published rows.

These tests establish the declared **single-writer, immutable-source, controlled
callback-failure model**. They do not test power loss, broker failover, concurrent
publishers, every malformed external source or a cross-system atomic transaction.
Recovery fixtures are owned small states, not fourteen-day fault-injection runs.
An unchanged existing Phase 12 manifest with six configuration snapshots was read
successfully through the original committed reader. No legacy layout migrated.

See [integration](phase17-closure-integration.md),
[checkpoint cases](../workloads/results/phase17-closure/checkpoint-recovery-validation.json),
[Kafka](../workloads/results/phase17-closure/kafka-transport-validation.json), and
[MinIO](../workloads/results/phase17-closure/minio-publication-validation.json).

## Costs, resources and negative findings

| Full selected window metric | Measured result |
|---|---:|
| Producer parse/serialize/ACK time combined | 1,278.893 s |
| Spark drain, validation and publication combined | 2,745.501 s |
| Validated baseline/delta reconstruction routines combined | 827.537 s |
| Nonempty micro-batches / median duration | 84 / 29,067.5 ms |
| Acknowledged source rows per drain second | 1,844.080 |
| Raw source-ledger Parquet | 1,154,238,793 bytes |
| Full daily canonical-state Parquet | 490,879,532 bytes |
| Transition Parquet | 2,188,602 bytes |
| Total committed Parquet / files | 1,647,306,927 bytes / 836 |
| All selected MinIO objects / commit descriptors | 1,648,876,130 bytes / 111 |
| Selected checkpoint | 106,951 bytes |
| Peak sampled Spark process-tree RSS | 2,728,542,208 bytes (2.541 GiB) |
| Minimum sampled guest free disk | 45,494,489,088 bytes (42.370 GiB) |
| Minimum sampled guest available memory | 4,187,942,912 bytes |
| Sampled guest per-run disk growth | 2,613,866,496 bytes |

The owner-approved VM disk is 64 GiB; RAM remains 6 GiB and CPUs remain two.
Host RAM is 18 GiB. No disk/RAM/time guard failed; all 2,417 host RSS and 483
guest samples were available. Sampling starts after Spark session creation,
can miss peaks and can double-count shared pages. It does not certify arbitrary
larger windows or horizontal scaling. The two-day stage's sampled peak was higher
(3,199,893,504 bytes); do not call the full-run peak the maximum of every stage.

Accounting for source archives once, all selected MinIO objects, reported Kafka
partition logs and checkpoint gives **2,659,717,473 bytes**. This is not filesystem
allocated space: Kafka indexes/global service overhead, JVM caches and experiment
logs are separate. The unchanged independent oracle retains 884,133,160 bytes of
validation evidence, not production history. Exact definitions are in
[resource measurements](../workloads/results/phase17-closure/resource-measurements.json).

Parquet is **1.811× logical CSV bytes** for this audited full representation;
provenance/identity preservation has a real storage cost. Both full states and
transitions are retained, so no delta-only storage saving is claimed. Validated
delta reconstruction grows from 1.725 to 134.526 seconds across this window;
these are end-to-end validation routines, not isolated query latency benchmarks.

Matched local callback reuse reduced median callback-total time by **27.70%**
(31.143 → 22.517 s; three pairs). It removes a redundant active-day segment
reread, **not** cumulative asymptotic growth, oracle scans or serial ordered digest
cost. The full service window has one repetition and no matched full-window
baseline. See [performance](phase17-closure-performance.md). The earlier SQLite
oracle and local Spark timings remain incomparable; simpler batch may be better
for daily snapshots. RQ2/RQ3 fair baselines remain Phase 20 work.

## Regressions and I1–I9

Final local checks: `make check` **954 passed** (12.47 s; lint/format passed),
`make api-test` **80 passed**, one existing warning (0.58 s), `make spark-test`
**71 passed** (92.37 s). Relative to Phase 17: **13 new unit cases**, **3 new Spark
cases**; API count unchanged. Actual service tests: **9 passed**, zero skipped.
Normal CI uses owned, service-free fixtures; it does not execute real-service tests.

Implementation CI for `50bf7ae30c5ea4e7f759e8b52ee059509236bacb` is
[run 38253122028](https://github.com/kongwoang/ConfigStream/actions/runs/38253122028),
completed/success; both quality and Spark jobs succeeded (954 unit, 80 API, 71 Spark).
The final artifacts-only delivery CI is checked after push and reported in handoff.
Development failures/startup aborts are preserved in
[validation](../workloads/results/phase17-closure/validation.json), not counted as
successful runs. All I1–I9 are **PASS_INTEGRATION for the selected RPKI window and
declared owned failure model**, with explicit limitations in
[invariant coverage](../workloads/results/phase17-closure/invariant-coverage.json).

## Remaining gates, rollback and Phase 18 boundary

Private bounded research is owner-authorized with RIPE attribution. Instructor
approval and unrestricted raw/normalized redistribution are **not established**.
Only aggregates, hashes, owned fixtures and code are committed. Actual publisher
validator build, operator intent and intraday changes are outside the evidence.
No larger RPKI window, DNS feature, RIS importer, API or alert engine was added.

The RPKI prerequisite for a separately authorized Phase 18 is closed. Before RIS
work, recheck permissions/resources and its full framing/expansion/occurrence
ledger: the Phase 16 bounded offset proof does not prove all MRT bodies, MP/ADD-PATH
or a full hour. Preserve exclusions and legitimate repeated occurrences; do not
use RPKI throughput to predict BGP performance. Existing full/reduced roadmap
graph tests pass; DNS postponement does not block retained-domain evaluation.

Rollback is additive: select `--processing-mode baseline`, stop only experiment
services, and retain committed objects/checkpoints/source evidence. Do not delete
volumes, rewrite previous results or reinterpret the unchanged `rpki-1.0` format.

**Vietnamese:** Đã kiểm chứng đúng 14 ngày RPKI qua Kafka–Spark–MinIO: 5.062.923
quan sát, 12.016 phần tử được thêm/xóa giữa các trạng thái đầy đủ; tái dựng khớp
oracle và phục hồi qua tám điểm lỗi có kiểm soát. Máy 18 GiB RAM chịu được cửa sổ
này sau khi ổ VM được phép tăng lên 64 GiB; không suy ra mọi workload đều chạy
được. Đây không phải 5 triệu lệnh cấu hình, cũng chưa chứng minh streaming tốt hơn
batch. Dừng tại Phase 17.1; chưa triển khai RIS/DNS.
