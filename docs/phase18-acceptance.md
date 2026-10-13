# Phase 18 — acceptance decision

**CONDITIONAL PASS — SERVICE PATH VALIDATED, FULL ORACLE/WINDOW PENDING**

Starting clean main: `229205d54df067f41ab86ad18ba2ecb1d6fbf847` (successful
CI run 38261071828). Implementation/accepted real-run code:
`314897f4ec79d34a93780096b6294fb9b19958cc`. Final documentation/evidence receipt
and exact CI are reported in the handoff; do not substitute an older CI run.

## What is accepted

- Complete hash/independent framing/CRC accounting of all twelve pinned archives:
  2,300,578 frames, 2,291,545 parsed UPDATE messages, 57 explicit exclusions.
- Complete **first five-minute** source occurrence projection, 370,141 events,
  through actual Kafka, Structured Streaming and MinIO, with zero missing/extra
  IDs against the declared ledger and independent NLRI/coordinate/identity checks.
- All 11,351 repeated-looking fingerprints in that archive remain represented;
  payload equality is not a logical deduplication key.
- Actual callback/checkpoint/conditional-publication fixture recovery, complete
  fixture archive replay and conflicting same-ID rejection; only committed reads.
- Additive code, strict types/policies, bounded records/chunks and existing regressions.

## Hard acceptance matrix

| Area | Result | Evidence / outstanding gate |
|---|---|---|
| Twelve source hashes | PASS | Matches original Phase 13 manifest; no downloads |
| Full framing / errors | PASS | Complete independent coordinates, EOF/CRC and all 57 exclusions |
| Native expansion | PASS_SCOPED | Owned vectors + first complete archive; frozen order/policy |
| Independent whole projection | CONDITIONAL | Binary NLRI/identity independent; attributes/fingerprints use pinned parser; full-hour ledger not produced |
| Multiplicity | PASS_SCOPED | First archive 370,141 distinct IDs; owned real-service four native IDs survive nine delivery copies |
| Full-hour repeated fingerprints | NOT_RUN | 118,172 is immutable Phase 13 reference, not remeasured here |
| Kafka / Spark / MinIO | PASS_SCOPED | Complete first archive, zero lag, exact committed projection, actual conditional retry |
| Recovery | PASS_SCOPED | Five actual owned service tests; not power loss/failover/multiwriter or full-hour recovery |
| Queries | PASS_SCOPED | Owned interval/peer/prefix/action/fingerprint/provenance tests and real minute/action/cardinality counts |
| Batch comparison | CONDITIONAL | Source oracle columnar output exists; full independent/matched timing baseline pending |
| Resources | PASS_SCOPED | Accepted pilot below registered disk/RSS/time guards; larger scope unapproved/unmeasured |
| Compatibility | PASS_REGRESSION | Configuration/RPKI code, original measurement artifacts and lake layouts unchanged |
| Four-dataset generalization | NOT_CLAIMED | DNS and Phase 20 not started |

## I1–I9 evidence levels

Final implementation regressions: `make check` **983 passed** (13.97 s,
lint/format passed), `make api-test` **80 passed** (0.64 s, one existing
Starlette warning), `make spark-test` **73 passed** (102.49 s). Relative to
the reviewed 954/80/71 baseline, this adds 29 unit and two Spark tests;
the five opt-in real-service tests are separate, not normal CI evidence. Those
five tests also passed on the exact implementation commit in 75.91 seconds.

| Invariant | Level | Scope / limitation |
|---|---|---|
| I1 provenance | PASS_INTEGRATION scoped | First archive source/manifest binding; full framing pinned independently |
| I2 replay identity | PASS_INTEGRATION scoped | Same-coordinate later delivery, full fixture replay, restart and conflicts |
| I3 multiplicity | PASS_INTEGRATION scoped | Distinct offsets/ordinals retained; real first-archive fingerprint repeats |
| I4 completeness | PASS_LOCAL / PARTIAL full scope | Complete gzip accounting and exact first projection; not full-hour event completeness or route state |
| I5 temporal honesty | PASS_INTEGRATION scoped | Source seconds preserved, owned extended precision, delivery timing separate |
| I6 committed visibility | PASS_INTEGRATION scoped | Partial output invisible, conditional retry/conflict, checksummed reads |
| I7 history reproduction | PARTIAL | Exact selected projection/NLRI, but full independent attributes/full hour unresolved; no RIB claim |
| I8 error accounting | PASS_LOCAL / PARTIAL integration | All 57 private framed exclusions; source counts in provenance; complete source error sidecar publication pending |
| I9 compatibility | PASS_REGRESSION | Old unit/API/Spark suites plus new RIS fixtures; no new full legacy-bucket read claim |

The machine-readable [acceptance](../workloads/results/phase18/acceptance.json)
and [invariants](../workloads/results/phase18/invariant-coverage.json) distinguish
executed results, historical references, estimates and NOT_RUN cases.

## Remaining stop gates / next action

**Do not begin Phase 19 or Phase 20.** First close Phase 18's complete independent
attribute/projection oracle, durable source-exclusion sidecars, exact three-file
and twelve-file ledgers, matched query/batch projections, remaining manifest fault
cases and larger-window actual service/recovery acceptance. Register larger RIS
runtime/resources independently; no inherited RPKI 120-minute authorization.
If a parser fix changes counts or accepted cases, preserve both policies/results
and seek a controlled amendment, never redefine the old 4,725,156/57 denominator.

Single collector/hour, shared pinned body-parser assumptions, local Spark,
unpaced sequential ingestion, sampled resources and physical cross-batch retry
copies limit generality. A faster batch result is acceptable. No novel event-
sourcing theory, complete route table, configuration commands or incidents are claimed.

## Rights, security and rollback

RIPE NCC/RIS are the source. Research/educational use and attribution were
reviewed against the [copyright statement](https://www.ripe.net/about-us/legal/copyright-statement/)
and [RIS terms](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/commercial-use/).
Private noncommercial research is the declared use; no unrestricted raw/normalized
GitHub redistribution permission or instructor approval is inferred. Only owned
fixtures, code and aggregate/source-coordinate metadata are public. Raw files,
peers/prefixes/attributes, checkpoints, credentials and logs stay private.

Disable new RIS commands/jobs to roll back use; retain isolated evidence. No
migration or deletion of configuration/RPKI data is necessary. Existing volumes,
topics and experiment prefixes are preserved; no system infrastructure changes.
After the final actual-service tests, Kafka, MinIO and Lima `k` were stopped.
Their volumes, source cache and experiment evidence were not deleted.

## Vietnamese executive explanation

Đã có đường RIS thực Kafka → Spark → MinIO cho **một archive 5 phút đầy đủ**:
370.141 occurrence, gồm 11.351 fingerprint nhìn giống nhau nhưng không bị xóa.
Logical ID dựa trên file hash, offset MRT và ordinal prefix, không dựa payload.
Toàn bộ 12 file đã kiểm tra framing/CRC và truy vết đủ 57 lỗi parser; chưa đồng
nghĩa đã xác nhận toàn bộ 4,7 triệu event qua pipeline. Oracle nhị phân độc lập
kiểm tra NLRI/peer/time/identity, nhưng phần attributes còn phụ thuộc parser
chuẩn. Vì vậy quyết định **conditional**, không tuyên bố full PASS.

Máy 18 GiB RAM chịu được pilot: Spark sampled khoảng 2,01 GiB, VM giữ 6 GiB và
còn hơn 41 GiB disk. Không suy ra toàn giờ cũng đạt cap hoặc streaming tốt hơn
batch. Cần đóng oracle/full-window/recovery gates của Phase 18 trước DNS;
giữ nguyên toàn bộ kết quả Terraform/RPKI và không mở thêm phạm vi.
