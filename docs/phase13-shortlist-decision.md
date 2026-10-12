# Phase 13 — evidence-based shortlist and Round 2 decision

## Executive decision

Recommend **four complementary sources**, conditional on an instructor-approved
scope of infrastructure **state and event history**, not exclusively file
configuration changes:

1. **A2, observed TerraDS Git sample** — direct configuration-version control.
2. **B1, RIPE RPKI archive** — operational authorization-state history.
3. **C1, OpenINTEL `.li` observations** — repeated infrastructure-state scale.
4. **D1, RIPE RIS** — naturally high-velocity routing observations.

The primary new policy workload is RPKI; the primary new streaming workload
is RIS. Terraform and DNS supply contrasting controls. These are four
different units, **not four kinds of configuration-file edits**. No adapter,
event schema, integration or deployment has been implemented in Round 1.

The original Phase 12 result **B remains unchanged**: direct configuration
history is sparse, and the million-snapshot experiment remains controlled
stress replay. The new results strengthen a broader infrastructure data
processing evaluation, not the claim that configurations change at telemetry
frequency. Evidence and primary citations are in
[the complete survey](phase13-dataset-survey.md).

## Scores, blockers and rankings

The disclosed rubric allocates 20 temporal authenticity, 20 natural scale,
15 access/reproducibility, 15 relevance, 10 fit, 10 complementarity and
10 licensing/security points. Full component scores are in
[scoring.csv](../workloads/results/dataset-feasibility/scoring.csv).
These are judgmental, with a ±5-point sensitivity band, **not statistical
confidence intervals**. Small probes do not justify global estimates.

| Candidate | Temporal authenticity /20 | Natural scale /20 | Access /15 | Fit /10 | Complementarity /10 | Total /100 |
|---|---:|---:|---:|---:|---:|---:|
| A2 Terraform Git | 19 | 7 | 15 | 10 | 9 | 83 |
| B1 RPKI | 18 | 15 | 14 | 8 | 10 | 87 |
| C1 OpenINTEL | 17 | 19 | 13 | 7 | 9 | 83 |
| D1 RIS | 19 | 20 | 14 | 5 | 10 | 84 |

Scores summarize evidence, not the reason to choose four. For example,
RouteViews overlaps RIS and PeeringDB has an access blocker. Numeric ordering
among the top four is not robust to rubric sensitivity. All 23 scores and
decisions appear in the generated
[all-candidate table](../workloads/results/dataset-feasibility/summary.md).

Separate rankings:

| Objective | Ranking | Qualification |
|---|---|---|
| Direct configuration history | A2, A3, A7 | A2/A3 locally recovered; A7 large archive not extracted |
| Operational policy state | B1, B2 | B2 unavailable here without agreement |
| Natural event velocity | D1, D2, C2 | Only D1 measured; C2 received no messages |
| Long historical evolution | D1, B1, B2, C1 | Directory/documented span, not complete gap-free coverage |
| Current practical feasibility | B1, D1, A2, C1 | Access, parsing and source-time evidence actually tested |

The generated [complementarity matrix](../workloads/results/dataset-feasibility/complementarity-matrix.csv)
uses explicit semantic heuristics: same domain/fit is less complementary;
cross-file/state/event domains score higher; RIS/RouteViews overlap is 2/9;
BGPStream versus its provider is 0/9; TerraDS-derived BPlan is not independent.
This matrix is **not a measured statistical correlation** and access blockers
override it.

## Candidate-level disposition

All candidates retain their decision and reason in the registry. In brief:

- **Primary:** A2, B1, C1, D1, for the distinct tested roles above.
- **Secondary:** A5 meaningful bug cases, A6/A7 Compose evolution/archive,
  A10 Helm release recovery, C2 live DNS events, D2 independent routing view.
  These need additional content/access validation, not immediate integration.
- **Baseline only:** A1 static TerraDS, A3 FRR/NGINX and A4 Batfish. Keep their
  evidence and limitations; they add no new natural velocity.
- **Academic reference only:** A13 BPlan methodology, A14 SWH archive services,
  B3 Two Campuses. B3 has no verified public raw download.
- **Access blocked:** B2 historical PeeringDB. Do not bypass its agreement or
  substitute current API data for historical snapshots.
- **Low priority:** A8 metadata-only ETH population, A9 static Kubernetes
  security artifact, A12 static/unclearly licensed examples, A15 paid/authenticated
  discovery services, D3 redundant access framework.
- **Rejected as natural-workload evidence:** A11 scripted audit logs. They
  remain valid experiment-generated logs, not observed production activity.

Do not delete existing datasets; stop new expensive TerraDS extraction and
do not download the 39 GB Compose mirror just for a bigger count.

## Two feasible research combinations

### Option 1 — configuration and state history

**A2 + B1 + C1**. Reuse actual Terraform snapshots, compare daily RPKI
authorizations and DNS address observations. This is coherent for audits,
state reconstruction, repeated-observation suppression, retention and batch
history analysis. RPKI offers stronger policy meaning than DNS; DNS offers
measurement ambiguity and substantial repeated observations. No natural
high per-file change velocity is established. Kafka/Spark are evaluated
for replay and scalable state/history processing, not justified by an
invented online configuration event rate.

### Option 2 — infrastructure state and event processing, preferred

**A2 + B1 + C1 + D1**. Adds an independently measured natural event workload:
one RIS collector-hour produced 4,725,156 peer-prefix events, approximately
1,312.54 events/s and a peak 181,868 events/minute. This creates a real reason
to study streaming ingestion, event-time handling and large event analytics.
The trade-off is scope and a future **native event path**, not coercing MRT
into per-file adjacent-version diffs. This option is preferable only if the
course accepts infrastructure state/events as the unifying domain.

PeeringDB could replace DNS for long-term infrastructure-policy evolution
after legitimate access and a substantive-field pilot. It is **not** part
of the current accessible shortlist. RouteViews could replace RIS for an
independent view, but selecting both initially adds volume with limited
semantic complementarity. ZoneStream could eventually complement daily DNS
with live events only after payload/rate/replay access is actually verified.

## Architecture implications — analysis only

| Dataset | Source adapter / identity | Snapshot processing | Event processing | Diff meaning | Future changes |
|---|---|---|---|---|---|
| A2 Terraform | Existing file incarnation and Git ordinal | Reuse current pipeline | None new | Normalized textual adjacency | None for baseline |
| B1 RPKI | Anchor/date/ASN-prefix-maxLength state | Canonical authorization sets, explicit baseline | Derived daily set transitions | Add/remove authorizations | State adapter and typed set analytics |
| C1 DNS | Measurement scope + RRset key + date | Canonical observed address set | Optional derived observations, not zone commands | Observed value-set difference | State adapter, measurement metadata, ambiguity flags |
| D1 RIS | Collector/peer/time/prefix/action/payload | RIB baseline only if separately included | **Native typed routing events** | Announce/withdraw, not ConfigDiff | Event contract/path and routing-specific analytics |
| B2 PeeringDB alternative | Object type + stable ID | Canonical non-PII fields | Derived substantive object changes | Record/property evolution | Legitimate access first, schema harmonization |

Reuse Kafka ingestion infrastructure, Spark event-time mechanics, committed
MinIO/Parquet, idempotent batch concepts, reproducible benchmark reporting and
metadata-only serving principles. **Do not assume** version-buffer rules,
router alerts, textual diffs or current schemas are appropriate for BGP.
Elasticsearch remains a derived projection; no raw payload exposure is needed.
Different typed datasets can share storage/measurement infrastructure without
merging their event semantics or counting them as one change total.

For F1: asset incarnation + extracted version gives dedup/order. For F2:
source scope + observation date + entity/state hash; compare only comparable
consecutive observations. For F3: retain source timestamps and event
multiplicity; fingerprints are diagnostic, not automatic discard keys.
Keep source time, observation time and ingestion time separate.

## Big Data validity

| Dimension | Actual support | What is not supported |
|---|---|---|
| Volume | Real static Terraform 2.60 GB; tested RPKI 909 MB CSV, RIS 427 MB MRT, DNS 252 MB uncompressed columns | Summing unlike units into configuration bytes or global estimates |
| Velocity | RIS parsed 1,312.54 prefix events/s from source timestamps | High router configuration-edit rate; DNS/RPKI event time inferred from daily scans |
| Variety | File history, authorizations, measured RRsets and routing events | All sources being direct configuration histories |
| Veracity | Pinned URLs, hashes/CRC, primary docs, explicit failures and counting rules | Perfect observation coverage, complete semantic interpretation or unrestricted licensing |
| History | Reconstructable Git and published long-lived observation archives | Decade-wide gap-free joins or deployment history |

For the small Git histories alone, a local script/database is enough; Spark
is primarily an architecture evaluation. For naturally large repeated-state
archives and high-rate MRT events, partitioned storage, streaming ingestion,
event-time handling and scalable analytics address real processing needs.
Our bounded probes establish feasible input scale/semantics, **not current
ConfigStream throughput on these domains**, production deployment value or
horizontal cluster scalability.

This remains one coherent academic topic if the questions concern reproducible
infrastructure history ingestion, deduplication, state-versus-event semantics,
storage amplification, event-time correctness and historical analytics.
It becomes incoherent if the sources are only appended to inflate record counts.

## Exact Round 2 proposal, not implemented

1. Obtain instructor scope approval and finalize source-specific license policy.
2. Freeze the same measured reference inputs: existing accepted Terraform history,
   14 RIPE-anchor days, two full `.li` days and one `rrc00` hour. No duplication.
3. Implement minimal separate typed state/event adapters; retain existing
   configuration contracts. No semantic Terraform/policy engine or deployments.
4. Verify counts and identities against Round 1 ground truth: 3,683 Terraform
   adjacent pairs; 12,016 RPKI tuple additions/removals; 4,091 changed common
   DNS address groups under the same filter; 4,725,156 parsed RIS prefix events
   with the same 57 excluded parser records. These are separate acceptance totals.
5. Replay through Kafka → Spark → committed MinIO with source time preserved;
   record physical replay rate separately from natural observation/event rate.
6. Evaluate idempotency, baseline-versus-change handling, missing dates,
   repeated observations, duplicate event multiplicity, event-time ordering,
   per-domain storage and batch analytics. Use separate result tables.
7. Extend only a justified contiguous window/collector/scope after resource
   and correctness gates; keep failures, licensing blockers and parser losses.
8. Compare snapshot/history workloads with the native event workload, not
   shared textual-diff accuracy or fabricated common security incidents.

Success requires exact domain-specific ground truth and a measured comparison
of useful processing behavior. It does not require four high anomaly counts.
No continuous live collection, cloud services, dashboard, ML or new deployment
platform is part of this proposal.

## Risks and stopping rule

OpenINTEL has noncommercial/share-alike conditions; RIPE and RouteViews terms
are not universally interchangeable with parser licenses; source Git licenses
remain separate from artifact licenses. One zone/collector/anchor has selection
bias. DNS variability and RPKI validator effects can masquerade as changes.
Archived schemas, missing dates, public/private ecosystem differences and
parser errors limit generalization. Future adapters must not silently bridge
gaps or make false adjacency. No statistical sample inference is made here.

**Recommendation: keep, but conditionally reframe.** Proposed future title:
*Scalable Infrastructure State and Event History Storage and Analytics*.
The repository has **not** been renamed. If the instructor requires exclusively
natural high-volume **configuration edits**, the new evidence does not solve
that requirement; retain the architecture-evaluation framing or reconsider
the topic. Do not start another broad data-search round before this decision.

## Tóm tắt gửi giảng viên

Ban đầu, lịch sử cấu hình Git có thật nhưng nhỏ và thưa: Terraform quan sát
10.188 revision từ 375 repo được chấp nhận; không thể coi bài stress 1 triệu
snapshot là dữ liệu sản xuất. Nhóm đã kiểm tra 23 nguồn/artifact và thử dữ liệu
thật có giới hạn: 14 ngày RPKI có 5.062.923 lượt quan sát nhưng chỉ 12.016
tuple được thêm/xóa; hai ngày DNS `.li` có 2.234.863 bản ghi và 4.091 nhóm
A/AAAA chung thay đổi; một giờ RIS có 4.725.156 sự kiện prefix, khoảng 1.313
sự kiện/giây theo thời gian nguồn. PeeringDB cần thỏa thuận truy cập; hai
phiên ZoneStream ngắn chưa nhận được thông điệp, nên chưa có số đo tốc độ.

Đề xuất dùng Terraform làm đối chứng cấu hình, RPKI cho trạng thái cấp quyền,
DNS cho trạng thái quan sát, RIS cho luồng sự kiện tự nhiên. BGP không phải
lệnh cấu hình; khác biệt DNS không chắc là sửa zone; thay đổi VRP không chắc
là thao tác quản trị. Nếu môn học cho phép, nên mở khung nghiên cứu sang
**lưu trữ và phân tích lịch sử trạng thái/sự kiện hạ tầng**, giữ Kafka–Spark–MinIO,
nhưng tách ngữ nghĩa từng nguồn. Round 2 phải chứng minh adapter, đếm đúng,
chống trùng, xử lý thời gian và analytics trên dữ liệu thật; hiện Round 1 chưa
tích hợp các nguồn mới vào pipeline. Nếu bắt buộc cấu hình tự nhiên có tần suất
cao, cần trao đổi lại tiêu chí hoặc cân nhắc đổi đề tài, không dùng BGP để
đổi tên sự kiện thành “config change”.
