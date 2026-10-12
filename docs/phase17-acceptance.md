# Phase 17 acceptance — bounded RPKI validation, service gates open

**PARTIAL — BOUNDED REAL-DATA STAGES VALIDATED.**

The independent fourteen-day batch oracle and a two-day local Spark/Parquet path
are validated. The required fourteen-day Kafka → Spark → MinIO experiment and
actual remote recovery remain **NOT_RUN**. This is not end-to-end PASS and not
completion of RQ2/RQ3 or the four-dataset research. Phase 18 was not started.

Starting clean `main`: `2d5772db73feba03376572f7dc6d9ef000112697`.
Implementation/validation revision and CI evidence are recorded below and in
[acceptance JSON](../workloads/results/phase17/acceptance.json).

## Delivered code

| Area | Modules / deliverable |
|---|---|
| Typed source contract | `schemas/rpki.py`; existing Phase 16 Evidence retained |
| Source and provenance | `workloads/rpki_source.py`, `rpki_reference.py`; bounded verified cached CSV |
| Independent oracle/batch | `workloads/rpki_oracle.py`; SQLite, source-coordinate maps, member inventories, SQL reconstruction |
| Transport | `messaging/rpki.py`; explicit registered topic, ACK-bound progress, bounded queue |
| Distributed processing | `spark/rpki.py`, `rpki_stream.py`; dedicated DataFrames/callback, no configuration buffer |
| Committed history | `lake/rpki.py`; additive registry, local and MinIO conditional metadata stores |
| Historical queries | `analytics/rpki.py`; membership, transitions, per-ASN activity and distinct members |
| Run/resources | `workloads/rpki_main.py`, `rpki_resources.py`; staged commands and sampling |
| Tests | owned CSV fixture; 32 unit and 5 Spark cases; opt-in live fixture |

No old production model, schema union, config Kafka topic, version buffer, lake
manifest/path, checkpoint, analytical output or API/search representation changed.
Makefile additions preserve its previous default target. Prior Phase 12–16 result
artifacts are untouched. New metadata/paths are explicitly versioned and isolated.

## Exact evidence and acceptance matrix

| Requirement | Evidence | Status |
|---|---|---|
| Pinned source integrity | all 14 SHA-256/size checks against Phase 13 | PASS |
| Independent raw accounting | 5,062,923 accepted rows; 0 excluded; 0 duplicate memberships within day | PASS |
| Independent canonical sets | 14 day inventories; union 367,204 members | PASS |
| Complete service daily coverage | 14 required; 0 service days executed | NOT_RUN |
| Local canonical daily coverage | 2 days, 360,226 and 360,506 members | PASS_LOCAL |
| Independent transitions | 13 boundaries; 7,096 additions, 4,920 removals | PASS_ORACLE |
| Pipeline transitions | 1 local boundary; 717 additions, 437 removals | PASS_LOCAL; other 12 NOT_RUN |
| Reconstruction | all 14 SQL states; 2 full/delta local Spark states exact | PASS_BOUNDED; 14 service states NOT_RUN |
| Kafka producer and consumer | ACK/resume owned unit fixture; actual broker unavailable | CONDITIONAL / NOT_RUN_LIVE |
| Spark scalability foundation | 720,732 rows in 12 bounded callbacks; no full-state collect | PASS_LOCAL; full-window capacity pending |
| MinIO committed visibility and retry | implementation + local fault fixtures only | NOT_RUN_LIVE |
| Checkpoint/failure recovery | actual owned local file-source Spark restart | PASS_OFFLINE; Kafka/MinIO NOT_RUN |
| Resource limits | no guard failure; sampled peak RSS ~2.29 GiB | PASS_LOCAL |
| Configuration compatibility | all previous service-free suites retained and passing | PASS_OFFLINE; live regressions pending |
| Scientific reporting | repetitions, transitions, prototype costs and pending gates separated | PASS |

All per-day hashes/digests are in [daily oracle](../workloads/results/phase17/oracle-daily-states.json).
Exact boundary-set digests are in [transitions](../workloads/results/phase17/oracle-transitions.json).
Private full ledgers support exact identity-set comparisons; counts alone were
not used to accept daily completion. Local day-one digest is
`accf40b30bcfd206e48ecf14e56e14e14688df25639d1358e8b50b3ca7790ee0`;
day-two digest is
`957b4a1a9ec92a83cef2af1c00c68c921985fdd57ba51f50c4b042f8d5d4bb91`.
The local boundary member digest is
`7044894b836c03e4fa623e0d79cee5848c1fdc6805b20fcd2e463a33473debea`.
These are policy-state observations, not administrator-command ground truth.

## I1–I9 coverage

| Invariant | Offline | Real evidence | Actual service evidence / final status |
|---|---|---|---|
| I1 provenance | PASS | 14 archive hashes; 2 committed local bindings | NOT_RUN / PARTIAL |
| I2 replay identity | PASS | local source-row reconciliation | NOT_RUN / PARTIAL |
| I3 multiplicity | PASS | 14 distinct daily observations | NOT_RUN / PARTIAL |
| I4 completeness | PASS | 2 independent local completed states | NOT_RUN / PARTIAL |
| I5 temporal honesty | PASS | dates preserved, no operator timestamps | N/A causal truth / PASS_OFFLINE |
| I6 committed visibility | PASS | local readers refuse incomplete states | NOT_RUN / PARTIAL |
| I7 reconstruction | PASS | 14 SQL + 2 Spark full/delta exact states | NOT_RUN / PARTIAL |
| I8 error accounting | PASS | 0 source exclusions; invalid fixture batches quarantined | NOT_RUN / PARTIAL |
| I9 compatibility | PASS | old suites pass | NOT_RUN / PARTIAL |

See [correctness](phase17-rpki-correctness.md) for the twenty mandatory failure
scenarios and the limits of oracle independence. No global exactly-once or
cross-system atomic transaction is claimed. A complete publisher CSV does not
prove complete worldwide RPKI state or cryptographic validator correctness.

## Tests actually run

- Before implementation: `make check`, **909 passed**, 14.28 s.
- Final audited `make check`: Ruff lint/format pass, **941 passed**, 12.66 s.
  **32** new unit cases; original unit baseline unchanged.
- `make api-test`: **80 passed**, 0.57 s; one existing dependency warning.
- Final audited `make spark-test`: **68 passed**, 67.00 s; **5** new cases beyond baseline 63.
- Total service-free passed: **1,089**. These are test cases, not source rows.
- `make phase17-integration-test`: **1 skipped**, 0.18 s; explicit live gate not
  enabled because Docker/Kafka/MinIO are unavailable. **0 live tests passed.**
- JSON/source/count/digest, link and fourteen archive-hash audits passed.
  Staged raw-file/secret and diff audits are recorded before commit. CI uses no
  live RIPE downloads and no fourteen-day input.

CI: pending implementation push at initial recording; the validation follow-up
records the exact tested commit, run URL, jobs and conclusions. A skipped local
service test is not a successful remote integration test.

## Resources, rights and negative evidence

Host: M3 Pro, 18 GiB RAM, initially ~629 GiB free disk. Docker container capacity
is unknown, not equivalent to host free space. Two-day local process-tree sampled
peak: 2,461,007,872 bytes; total experiment peak sampled ~1.55 GiB. No new source
download, service installation, paid query, source execution or volume deletion.
[Experiments](phase17-rpki-experiments.md) gives complete costs, safety limits,
failed initial whole-day callback and reproduction commands.

The independent all-fourteen-day batch path worked with much smaller parent RSS
than the local Spark path, but projections, caching and tasks differ. This is not
a fair speed/efficiency ranking. The system's serial checksum/cumulative scans
remain bottlenecks, and full fourteen-day streaming capacity is unmeasured.

Owner authorized bounded Phase 17 development; instructor approval is not
evidenced. RIPE is attributed; private research-use basis is documented, but
specific raw/normalized redistribution rights and upstream validator configuration
remain unresolved. Only aggregate results and owned fixtures are published.

## Remaining gates and stop policy

1. Provide authorized Kafka/MinIO services and measured container memory/disk,
   retention and isolated prefixes; do not install/reconfigure automatically.
2. Execute the owned live fixture, actual conditional publication, partial output
   visibility and checkpoint interruption/retry. Extend fault coverage where the
   existing fixture does not exercise a required remote case.
3. Advance one complete service day, two days, then fourteen. Verify exact source
   maps, every member set, all thirteen boundaries and every reconstructed state.
4. Stop for any oracle conflict, incomplete-state transition, missing identity,
   incompatible old reader, publication failure or resource breach. Preserve the
   failed evidence; do not change expected counts or weaken the oracle.
5. Confirm public-output rights and instructor scope approval before larger or
   external-publication milestones. Failure leaves private/offline work valid
   but blocks that dependent milestone; Track R remains DNS-optional.

**Phase 18 prerequisite:** close the declared Phase 17 service/reference gates or
obtain a controlled scope amendment; never represent local fixtures as remote
acceptance. No DNS/RIS ingestion, alerting, API or routing reconstruction was added.
Rollback is to stop the new opt-in commands and retain private output, not to
delete old evidence or migrate configuration history.

## Tóm tắt tiếng Việt

Đã xác minh đủ 14 file nguồn và tính độc lập 5.062.923 dòng, 14 tập trạng thái,
12.016 thêm/bớt; đây không phải 5 triệu thay đổi cấu hình. Spark cục bộ đã xử lý
hai ngày, khớp chính xác 720.732 dòng và 1.154 thay đổi, tái dựng đúng cả hai tập.
Thiếu Docker/Kafka/MinIO nên chưa chứng minh đường dịch vụ đầu-cuối hoặc phục hồi
phân tán. Máy 18 GiB RAM chịu được thí nghiệm cục bộ này (~2,29 GiB RSS lấy mẫu),
nhưng tài nguyên container và toàn bộ cửa sổ streaming vẫn cần đo. Kết quả được
ghi là **PARTIAL**, không nâng thành PASS; chưa bắt đầu Phase 18.
