# Phase 15 — pre-registered experimental design

All experiments here are **future**, unless explicitly identified as reused
Phase 12/13 characterization. No performance result or new integration is claimed.
Stable experiment IDs correspond to
[experiment-matrix.json](../workloads/results/phase15/experiment-matrix.json).

## Four evaluation layers

**A Source:** hash/size/coverage, raw unit count, unique entities, repeated
observations, meaningful qualified changes, source-time velocity, missingness,
parser exclusions. **B Correctness:** exact ID→payload maps, occurrence multiplicity,
missing/extra IDs, checksums, gap handling and restarted output.
**C Storage/query:** retained source, transport, raw lake, analytical lake,
checkpoint, manifests/indexes, scratch peak, files and exact query/reconstruction
cost. **D Processing/resources:** producer acknowledgments, accepted unique units,
publish/stream/drain/start/recovery durations, lag, Spark batches and RSS/CPU.

Natural source rate is only justified for RIS occurrences; daily RPKI/DNS have
observation cadence, not equivalent natural change-event arrival rates. A replay
rate is a physical experimental load. N/A is required for unknown authoritative
DNS edits, complete route-state reconstruction, deployment frequency and
unmeasured serialized sizes—not zero.

## Input and baseline fairness

Freeze exact original artifact URLs/hashes in the
[Phase 13 manifest](../workloads/results/dataset-feasibility/probe-manifest.json);
reuse the accepted Phase 12 sample. Preserve parser/counting policy and exclusion
ledger. No sampling replacement, artificial empirical-history duplication, global
extrapolation or accelerated replay relabeled as natural velocity.

Baseline B0 is independent Python parsing and direct append-only Parquet;
B1 uses the same normalized projection and local analytical SQL (DuckDB **only
if approved/pinned for these queries**, not a production dependency); B2 Spark
batch; B3 Kafka–Spark streaming. B4 full states, B5 canonical state reuse with
observation references, B6 baseline/deltas, B7 checkpoints+deltas are **storage
representations**, not separate competing engine implementations. B8 deliberately
unsafe payload/time dedup is a fixture control for RQ1, never a recommended pipeline.

For batch-versus-stream comparisons share a pinned Python parser output/expansion
ledger where possible; separately measure raw parser cost. Match accepted logical
projection, compression, partitions/file target, query, input selection and
correctness. Direct-Parquet and streaming raw-input copies must be recorded in
all-in cost rather than hidden. B0/B1 can be faster; no input inflation to favor Spark.

Predeclare host limits before runs: one machine, local[2] Spark, at most two
query threads/cores for matched local SQL, one active measured job, same Docker
CPU/memory limits recorded for each run, no paid services. Reserve total measured
process/container RSS within 12 GiB on the recorded 18 GiB host; stop at sustained
budget breach rather than inducing swapping. Recheck actual host/container limits.
Reserve 15 GiB/run, 50 GiB experiment cap, 20 GiB always free and at most half
initial free disk. JVM Xmx is not total RSS. If a mode cannot fit, report failure
under that budget or amend all matched configurations before further runs.

Three measured repeats per matched configuration; seed 42 shuffles run order.
Record every run including failures. Keep cold/startup-inclusive and warm/steady
measurements separate; no privileged OS cache purges. Describe “cold” as fresh
process/non-prewarmed analytical inputs unless filesystem cache eviction is
actually established. Report each value, median and range, paired ratios and
cache/resource details. Three repeats do not warrant a population CI or p-value.

## Table E — storage/reconstruction experiments (RQ2)

| ID / dataset | Representation and preserved information | Required queries | Measurements / fairness |
|---|---|---|---|
| E2T Terraform | Existing accepted raw version references plus normalized contents and adjacent text diffs | Retrieve named accepted version; versions/asset; adjacent pair metadata | Unchanged reference, no new compression claim; raw cannot be reconstructed from masked normalized text |
| E2R RPKI | Full canonical daily sets; initial baseline+deltas; baseline+checkpoints every 7 accepted consecutive days+deltas | Reconstruct each selected day; membership for declared tuple; daily/per-ASN add/remove totals | Exact set digest every day; same source/tuple projection, compression, rows/file; descriptor/recovery overhead included |
| E2D DNS | Full observed rows; canonical address sets+source-coordinate/status/time/worker observation references; state-value reuse; qualified differences | Reconstruct every declared group-day projection; stability/common-group differences; coverage/status audit | Lossless only relative to declared projection with raw source retained; time/quality references cannot be removed; raw-row queries cannot be compared to a smaller address-only query |
| E2B RIS | Immutable source artifacts referenced once; lossless normalized occurrence projection+ledger; per-minute summaries | Exact occurrences in interval; per-minute action counts; top peer/prefix activity | Summary is lossy and not a replacement for occurrence history; compare query acceleration with extra summary bytes and build time, not “compression” by dropping events |

Retention policy for experiments: retain the unique verified source artifacts
once privately and reference them from alternative representations. Store raw
Kafka delivery history only if evaluated, count it; no silent multiplication of
archive bytes across designs. Keep each representation's logical definition fixed.
Use date/scope partitions, not entity-per-directory; measure small files and
manifest overhead. Optional compaction is a separately timed, immutable new run
with equivalence checks and extra peak disk; it is not required before primary
experiments and cannot silently replace earlier measurements.

For RPKI every complete source day must reconstruct exactly. Deltas without
checkpoints trade fewer stored rows against replay/read chains; do not predict
bytes from row reduction. For DNS value reuse must retain every qualified
observation, including equal states. Missing/failed groups remain status evidence,
not empty sets. For RIS aggregation loses occurrence identities/multiplicity
details and cannot satisfy the same lossless representation requirement.

Preference rule, engineering choice not natural law: a lossless representation
is preferable only if it passes RQ1, saves **at least 20% analytical bytes** versus
the matched full projection and costs **no more than 2× median** reconstruction/
query time for **each** registered required query. All-in bytes include shared
archives once, analytical data, metadata and allocated recovery state; report
cases where shared raw cost overwhelms analytical savings. No universal compression
ratio: Parquet includes schemas, repeated values, multiple projections and files.
If nothing qualifies, retain full snapshots/logs and publish the negative result.

## Table F — processing experiments (RQ3)

| ID / dataset | Initial window and unit | Modes / limits | Metrics and stop |
|---|---|---|---|
| E3T Terraform | Existing 10,188 accepted versions, no remine | Existing path/reference acceptance only | Regression/units context; no matched throughput ranking against prefix occurrences |
| E3R RPKI | Exact 14 days; source rows and completed set descriptors separately | B0, B2, B3 under matched resources | Parse/publish/drain/start time, accepted rows, complete observations, transition cost/RSS; no low-latency requirement invented for daily publication |
| E3D DNS | Exact 2 `.li` partitions; raw rows versus grouped observations separately | B0/B1 query, B2/B3 only after DNS gate | Canonicalization expansion/reuse, status cost and startup overhead; no live DNS stream claim |
| E3B RIS | Actual nested 5, 15, then 60 minutes; peer-specific unicast prefix occurrences | B0/B2/B3; 0.5/1/2× fixed hour mean physical load, uniform and source-burst-preserving schedules distinguished | ID correctness, accepted occurrences/s, serialize/parse cost, final/peak offsets lag, batch durations/RSS/CPU, recovery time; stop resource breach or unexplained identity mismatch |

For RIS select archives 00:00, then 00:00/00:05/00:10, then the measured hour;
pin those manifest entries, not arbitrary first N parsed events. Rate targets
derive from 4,725,156/3,600 **only as physical load controls**; report achieved
producer and accepted rates separately. Burst-preserving schedule rescales
source-time interarrival intervals without erasing same-second multiplicity;
producer backpressure and actual arrivals are logged. This tests local processing,
not source generality or production network collection.

Define `accepted_rate = distinct accepted source IDs / (stream_ready to final
validated commit time)` for the run, including drain; separately report startup
and total command time. Also record physical records/Kafka bytes, source records
expanded into events, duplicate deliveries and validation exclusions. Native
Spark branch input/processedRowsPerSecond is **not** the logical occurrence rate.
Kafka lag is offset lag per selected partition with recorded start/end offsets,
not seconds of natural history. Publication latency is arrival-to-visible-commit;
historical source-to-arrival age is not live processing latency.

At 1× physical target: require RQ1, final selected-offset lag zero, no unexplained
watermark loss, resource compliance and drain ≤10% producer duration. A failed
capacity gate stops larger windows, not publication of a negative result. 2× is
a saturation experiment, not a mandatory superiority claim. Recovery claims require
failed-run/restart comparison against a clean ledger, not simply a running query.

## E1 — common correctness campaign (RQ1)

Use [I1–I9 and oracles](phase15-correctness-invariants.md) before costs.
Perturb project-owned fixtures for retry, ties, shuffle, delayed observation,
partial/missing day, parser exclusion and failures around publication/checkpoint.
For real input run clean and selected bounded restart/retry cases; freeze all
outputs and exclusions. Report zero unexplained missing/extra IDs or declare
failure. Deliberately unsafe dedup must lose the equal-payload/different-coordinate
fixture, demonstrating that the suite can falsify an incorrect implementation.

## E4 — DNS generalization, not another monitoring product

**Hypothesis:** shared identity/completeness/publication invariants transfer to
measurement observations while DNS-specific qualifiers prevent false deletion
and causal claims. RPKI cannot alone test this: complete canonical membership
permits absence→removal; a complete DNS partition does not establish the state
of every unobserved/unsuccessful query.

1. Preserve all source rows and coordinates in the private reference ledger.
2. Reproduce original successful address-group comparisons independently.
3. Retain source times/status/worker/scope evidence while reusing equal value sets.
4. Inject missing partition/chunk, timeout, successful empty with explicit fixture
   evidence, changed worker, duplicate row and delayed completion into **owned
   fixtures only**; keep empirical two-day results unchanged.
5. Check unchanged observations remain represented; missing/failed groups never
   become empty sets/deletions; late completion produces only validated derived
   runs and readers never see a partial analytical publication.
6. Measure reconstruction/storage/query cost and show exactly which shared
   invariants hold, which DNS qualifiers are necessary, and which questions are N/A.

Success: exact row/group/status evidence; no false removal/authoritative-edit
label under negative fixtures; reconstruction exact for the declared projection;
same publication/recovery tests usable with typed expectations. **Failure:**
DNS requires bypassing the completeness invariant, drops status/time references,
or contributes only an IP-change count. Then postpone DNS and amend K3/four-role
claims, not add root-cause inference. Unknown resolver/vantage, causal edits and
successful-empty cases without source evidence remain unanswerable.
