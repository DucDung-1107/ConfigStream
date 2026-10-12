# Phase 14 — GO / reduced scope / NO-GO decision

## FINAL DECISION: GO — REDUCED SCOPE

**Recommendation, not implementation authorization:** proceed only with an
instructor-approved, budgeted **two-new-domain** experiment: RIPE RPKI observed
authorization sets and RIPE RIS routing occurrences. Retain the completed
Terraform experiment as a reference. **Do not implement the full four-dataset
vision.** Postpone DNS, new serving endpoints, route-state reconstruction,
RPKI/BGP validation joins, dashboards and distributed deployment.

Primary reason: RPKI versus RIS creates a meaningful, falsifiable contrast between
redundant policy-state observations and event multiplicity, within one network-history
preservation/cost problem. DNS adds a third normalization/measurement/legal problem
without being necessary to answer the selected questions. The full design has
higher scope risk than its marginal empirical value. A negative batch/streaming
or delta-storage result is an acceptable research outcome.

Recommended exact academic title:
**ConfigStream: Evaluating Storage and Analytics for Network Policy and Routing
Event Histories**. Do not rename the repository. “Scalable” can describe the
architecture/design goal, not proven multi-worker scalability. The broader title
“Scalable Infrastructure State and Event History Storage and Analytics” is too
general for the selected two-source MVP.

## Findings and the unresolved investment question

The full vision can be one project **only** if its contribution is a common
preservation/publication/evaluation contract, not common Kafka services. In the
reduced vision this is manageable: complete daily sets and immutable event
occurrences share provenance, retry accounting, committed historical reads and
failure tests; their payloads, comparisons and queries remain typed.

Central problem: **preserve observed policy states and operational occurrences
correctly under replay/recovery, and measure retention/query/ingestion costs
without erasing their temporal differences.** Target users are measurement and
audit researchers, not deployment automation users. See
[research vision/RQs/related work](phase14-research-vision.md).

| Question | Proposed evidence | What would refute the hypothesis |
|---|---|---|
| RQ1: correctness under retry/disorder/gaps | Exact daily set digests and RIS source-coordinate multisets, failure injection | Any unexplained missing/extra occurrence, false gap transition or non-reproducible output |
| RQ2: storage versus reconstruction cost | Same-input full daily sets, baseline+transitions and seven-day analytical checkpoints | Delta layout saves no bytes or query cost outweighs its declared engineering threshold; retain the negative result |
| RQ3: event-path cost/capacity versus batch | Actual nested RIS windows, matched queries, physical load points, lag/RSS/batch/file metrics | Local streaming cannot meet the declared 1× replay capacity gate, or offers no useful recovery/latency benefit over batch |

Engineering/empirical contribution, not new algorithmic novelty: typed source
identity/completeness, explicit recovery failure cases and within-domain
storage/query/batch-stream trade-offs. Spark, event sourcing, BGPStream, RPKI
analysis and active DNS measurement already address much of this territory.
Simply adding sources would not be a contribution. The [related-work table](phase14-research-vision.md#related-work-novelty-limits-checked-primary-sources)
constrains every novelty statement.

## Empirical Big Data assessment

- **Volume:** measured RPKI 909,372,097 CSV bytes over 14 days; RIS 427,068,595
  MRT bytes and 4,725,156 parsed occurrences over one hour. These can exercise
  retention, serialization, file layout and analytical scans but fit on one host.
  Longer-retention planning scenarios are not already collected natural volumes.
- **Velocity:** RIS measured 1,312.5433 occurrences/source second and 181,868 in
  the busiest source minute. RPKI is daily; Terraform remains sparse. No statement
  about natural router configuration-edit frequency is strengthened.
- **Variety:** complete set observations versus native occurrence logs meaningfully
  test different temporal preservation policies, without four parsers for variety's sake.
- **Veracity/history:** hashes, source coordinates, filters and explicit errors
  support reproducibility. RPKI validator changes and collection gaps remain;
  BGP is one collector's observations; updates alone do not reconstruct full routing
  state. Daily state comparisons cannot recover intraday actions.
- **Architecture relevance:** object storage and columnar analytical retention
  are sensible; Kafka enables controlled replay/backpressure/recovery; daily RPKI
  streaming is not inherently necessary. Spark superiority is unproved and must
  be compared with simple batch. A local experiment cannot establish horizontal
  scalability, production reliability or operational incident detection.

All counts reference [committed measurements](phase14-research-vision.md#evidence-anchors-measured-not-new-phase-14-experiments).
The [feasibility calculations](phase14-feasibility.md) separate measured inputs,
extrapolated scenarios, assumptions and unknown Spark/sink costs. Phase 12's
**B — architecture-evaluation only** remains unchanged for configuration history.
The new direction is materially stronger for **network-history systems evaluation**,
not a rescue of naturally high-frequency configuration telemetry.

## Table F — mandatory investment gates

PASS here means the review establishes a feasible design/evidence basis, **not**
an implemented acceptance result. Conditional gates must close before their
expensive dependent milestone; a score cannot override them.

| Gate | Result for reduced scope | Evidence | Blocker | Action |
|---|---|---|---|---|
| 1 Academic coherence | **CONDITIONAL** | One preservation/cost problem, three falsifiable RQs; prior-art limits | Course may require operational analytics rather than systems evaluation | Obtain instructor acceptance of title, users, RQs and negative-result outcomes before coding |
| 2 Semantic integrity | **PASS (design)** | Typed state/event contracts; complete-state barrier; coordinate multiplicity | No new contracts/oracles implemented | Prove offline before service integration; reject fake ConfigSnapshot BGP |
| 3 Big Data substance | **PASS, bounded** | Genuine measured RIS velocity and repeated-state volume | One host/hour/anchor; no broad generality or Spark necessity | Evaluate actual local capacity/storage/query costs and simple baselines, no cluster claim |
| 4 Architectural feasibility | **CONDITIONAL** | Existing committed lake/runtime/metrics reusable; config-specific allowlists identified | Typed event ingestion and additive lake registry are real work | Preserve old schemas/defaults/checkpoints; validate typed registration/recovery |
| 5 Resources/effort | **CONDITIONAL** | 18 GiB host; ample recorded disk; explicit 110/194/346 h accounting | No actual team time budget/deadline/container limits supplied | Confirm available hours; inspect limits and five-minute byte/RSS pilot; retain project if budget inadequate |
| 6 Evaluation feasibility | **PASS (design)** | Exact source manifests/counts, independent set/coordinate oracle plan, within-domain baselines | Phase 13 aggregate totals alone are insufficient identity ground truth | Build coordinate/digest ledger and fixtures; correctness before throughput |
| 7 Reproducibility/legal safety | **CONDITIONAL** | Public cached inputs; clear pointers/hashes; no raw redistribution | Exact raw/normalized publication grants not established; future terms/access risk | Cache-only data, owned CI fixtures and aggregate publication; resolve any required raw/demo permission first |
| 8 Scope risk | **PASS for reduced scope** | Two new typed paths, Terraform reference only, DNS postponed | Four-domain version risks disconnected demos and extra legal work | Freeze exclusions; stop if ROV/RIB/DNS/API is required without separate approval |

If gate 1 fails because the course demands one domain-specific operational result,
do not quietly add security analytics to pass. Revisit the academic topic with the
instructor. If a real distributed cluster is mandatory, this local MVP is insufficient;
price/approve that separate requirement or select **NO-GO — RETAIN EXISTING PROJECT**
if allowed, otherwise discuss topic reconsideration. If raw-data publication is
mandatory but permission is unresolved, gate 7 blocks that implementation/demo.

## Title alternatives, no rename

| Proposed framing | Accuracy/specificity | Academic strength and risk | Current-code relationship |
|---|---|---|---|
| Empirically Grounded Configuration-History Architecture Evaluation | Narrow, fully implemented and honest | Strong reproducible engineering; weak natural temporal scale | Exact Phases 0–12 scope; safest fallback |
| Infrastructure State-History Storage and Analytics | Fits Git/RPKI/DNS observations, not BGP event reconstruction | Meaningful state redundancy/query problem; no high natural velocity established | Needs typed state semantics, retains version path |
| Infrastructure State and Event-History Storage and Analytics | Technically possible but broad | Highest incoherence/overrun risk; no novelty from heterogeneity | Several new domain paths, least direct reuse |
| **Evaluating Storage and Analytics for Network Policy and Routing Event Histories** | Narrowest accurate RPKI/RIS scope, explicitly evaluation | Real natural velocity plus policy-state contrast; no causal/security claims | Reuses platform contracts/runtime, not ConfigDiff/version state for RIS |

## Instructor approval questions (ask before implementation)

1. Is a common preservation/storage/cost architecture over policy states and
   routing events accepted as one project, or is a domain-specific operational
   question such as route validation required?
2. Are real bounded inputs, failure/correctness tests and fair batch baselines
   sufficient, including a negative performance result, rather than a tool demo?
3. Is one measured natural high-velocity source sufficient when the other is a
   daily policy history? Must multiple collectors or longer windows be processed?
4. Is single-host local Spark acceptable, or must an actual multi-worker cluster
   and horizontal scaling experiment be shown?
5. Can raw inputs remain private/source-downloadable, with owned fixtures and
   aggregate statistics public, given third-party redistribution terms?
6. What actual person-hour budget is available, and can a small two-domain system
   with stronger experiments be preferred to four partially implemented domains?

Do not assume the answers. If the instructor accepts the existing empirical
configuration evaluation and time is scarce, **A is cheaper and defensible**.
Reduced expansion is not a requirement to justify sunk cost.

## Future roadmap — only after gate approval

Selected MVP: RPKI complete-state import/transitions/reconstruction plus RIS
occurrence ingestion/aggregates; common provenance and committed storage/recovery;
no new search/API, live collectors, routing-state engine or new policy alerts.
Use already cached inputs. Stage costs partition the same 194 expected hours,
not additional work: four hours of P5's oracle inspection are front-loaded into M1.

| Milestone / expected labor | Dependencies / deliverables | Acceptance and research evidence | Kill / rollback criterion |
|---|---|---|---|
| M1 contracts and offline correctness, 22 h | P0,P1 + 4 h from P5; instructor/legal scope, typed specs, own set/event fixtures and coordinate feasibility | Same-content distinct occurrences survive; same coordinate retry suppressed; incomplete-day transitions refused; no config-schema changes | No scope/legal/budget agreement, unresolved coordinate ambiguity after the timebox, or universal optional-field schema required: stop before services |
| M2 first real source, RPKI, 40 h | P2,P3,P4; typed publication, daily completeness, exact sets/transitions | All 14 daily canonical sets and per-date digests agree; 12,016 add/remove tuples; partial-date/gap negative tests; old lake schemas still readable | Any unexplained set/transition mismatch, corrupted old manifests or inability to publish complete observations: no event-path expansion until fixed within budget |
| M3 distinct event source, RIS, 50 h | Remaining P5,P6,P8; 5-minute pilot then pinned hour, coordinate oracle and recovery tests | Exact parsed occurrence multiset (original 4,725,156-policy baseline), 57-error accounting, 118,172 fingerprints not blindly removed, checkpoint/partial-commit failures correct | Lost multiplicity, silent parser exclusions, shared commit contract cannot preserve both types, or run exceeds reserve: stop/retain validated RPKI stage, no performance claims on incorrect data |
| M4 real experiments and baselines, 56 h | P7,P9,P10; typed queries, layouts, three repeats and load points | RQ2 reconstruction/bytes/file/query trade-offs; RQ3 same-input batch/stream and capacity metrics, failed runs retained | Exact correctness fails, disk/RSS guard breached, 1× target cannot drain within declared gate: report limit; no 24-hour or multi-domain expansion |
| M5 report and regression, 26 h | P11,P12; evidence bundle, claim audit, existing regressions/CI | Reproducible source manifests, units/uncertainty, simple baselines, aggregate safe publication, instructor-ready narrow result | No meaningful trade-off or unsupported claims persist: narrow to architecture evaluation, do not declare universal/production scalability |

Total selected investment: **110 best / 194 expected / 346 worst person-hours**.
Full four-domain estimate: **158 / 280 / 498**. If substantially less time is
available, select existing-project closeout or explicitly smaller state-only
fallback; do not cut identity, failure testing or baselines to fit four adapters.
No subsequent phase is started by this review.

## Vietnamese executive explanation

**Đề xuất: GO — REDUCED SCOPE, không làm cả bốn nguồn.** Điểm yếu ban đầu là
history cấu hình thực khá thưa: Terraform chỉ có 10.188 revision quan sát được
trên 6.505 file incarnation, khoảng 12,28 MB; benchmark một triệu snapshot vẫn
là stress replay. Khảo sát mới cho thấy RIS có 4.725.156 sự kiện peer-prefix
trong một giờ nguồn, khoảng 1.312,54 sự kiện/giây; RPKI có 14 trạng thái ngày,
5.062.923 dòng quan sát lặp lại nhưng chỉ 12.016 tuple thêm/bớt. Đây là hai loại
bằng chứng khác nhau, không phải hai loại “thay đổi file cấu hình”.

Nên tập trung vào **lưu giữ đúng lịch sử trạng thái chính sách mạng và sự kiện
định tuyến**, kiểm tra replay/retry, tính đầy đủ, chi phí lưu trữ và truy vấn;
giữ Terraform làm đối chứng đã hoàn thành, hoãn DNS. Kafka/Spark/MinIO là kiến
trúc được đánh giá, không mặc nhiên tốt hơn script batch; phải có baseline đơn
giản và chấp nhận kết quả âm. Không suy luận commit là deployment, RPKI diff là
lệnh vận hành, hay BGP update là config change. Chưa có RIB nên không hứa khôi
phục toàn bộ bảng định tuyến.

Phạm vi này cần khoảng **110–346 giờ công, mức kỳ vọng 194 giờ**, gồm test,
fault/recovery, benchmark và báo cáo. Chỉ nên đầu tư nếu giảng viên chấp nhận
bài toán đánh giá hệ thống này, local Spark và cách công bố dữ liệu an toàn.
Nếu yêu cầu cluster thật, một ứng dụng vận hành chuyên sâu, hoặc thời gian không
đủ, nên giữ và trình bày Phases 0–13 thay vì mở rộng cho đẹp số liệu. Big Data
tự nhiên nằm rõ nhất ở **sự kiện định tuyến được quan sát**, không nằm ở tần suất
sửa cấu hình từng thiết bị.

## Review validation and work boundary

The five review documents add distinct vision/RQs, architecture/contracts,
feasibility, risk/legal and decision/roadmap analyses. Compact machine-readable
records are under `workloads/results/phase14/`. Calculations can be recomputed
from the pinned committed JSON; unknown sink/runtime values remain null.

Local review checks on 2026-10-12: `make check` passed Ruff lint/format and **823
unit tests** (13.24 s); `make api-test` passed **80** (0.57 s, one existing
Starlette/httpx deprecation warning); `make spark-test` passed **60** service-free
transformations (48.62 s). Six JSON records, pinned source hashes, observed totals,
scenario arithmetic, package/milestone sums, critical path and local document links
were checked with an ephemeral offline Python verification. No analysis script or
new runtime code was added, so no new offline test suite is claimed.

The inspected baseline's [CI run 38191154356](https://github.com/kongwoang/ConfigStream/actions/runs/38191154356)
was successful. That is **baseline CI**, not an advance claim about the review
commit; the completed review commit's workflow result is reported in the final
handoff after push. Machine-readable local validation is in
[decision-matrix.json](../workloads/results/phase14/decision-matrix.json).

No production/test code or Phase 10–13 measurement is changed. No new integration
test, corpus download, Docker service, adapter, schema, topic, job, lake zone,
endpoint, deployment or benchmark execution is claimed. Validation results and
completed workflow provenance are recorded in the final handoff / review validation
record after checks, not assumed successful in advance.
