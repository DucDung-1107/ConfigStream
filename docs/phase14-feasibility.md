# Phase 14 — quantitative feasibility and investment budget

Inputs and arithmetic are in
[feasibility-estimates.json](../workloads/results/phase14/feasibility-estimates.json).
Source JSON hashes pin the measured RPKI/RIS/DNS inputs. Byte units below are
decimal MB/GB; a GiB reserve is binary. Figures marked scenario are **not measured
future results, forecasts or confidence intervals**. No new data was downloaded.

## Window sizes and information content

| Window | Evidence class | Compressed / physical input bytes | Logical source bytes | Source units | Derived information |
|---|---|---:|---:|---:|---|
| RIS 1 hour | Measured | 73,771,296 | 427,068,595 MRT | 4,725,156 prefix occurrences | Counts/actions/bursts; no complete route state |
| RIS 24 hours | Nominal same-hour-density scenario, ×24 | 1,770,511,104 | 10,249,646,280 MRT | 113,403,744 occurrences | Unknown future action mix; no predicted changes or unique prefixes |
| RPKI 14 days | Measured | 63,816,988 | 909,372,097 CSV | 5,062,923 repeated observations | 12,016 add/remove tuples, 13 daily comparisons |
| RPKI 30 days | Nominal same-day-density scenario, ×30/14 | 136,750,689 rounded | 1,948,654,494 rounded CSV | 10,849,121 rounded observations | 26,805 rounded transitions **only if** mean daily-boundary churn repeats: 12,016×29/13 |
| DNS 2 days | Measured | 98,828,670 Parquet | 252,307,526 uncompressed columns | 2,234,863 rows | 4,091 changed common address groups; 485 new/676 absent groups separately |
| DNS 7 days | Nominal same-day-density scenario, ×7/2 | 345,900,345 | 883,076,341 uncompressed columns | 7,822,021 rounded rows | Unknown; do not extrapolate six daily change sets from one comparison |
| Existing Terraform history | Measured, reused | No single compressed historical artifact measured here | 12,278,085 UTF-8 bytes | 10,188 revisions, 6,505 incarnations | 3,683 adjacent pairs; 375 accepted repositories |

The logical columns are deliberately **not interchangeable**: CSV text, MRT binary,
Parquet uncompressed column accounting and UTF-8 config content. DNS canonical
A/AAAA JSON was only 20,140,457 bytes across both days, a different projection
from its complete measurement rows. See original
[probe summary](../workloads/results/dataset-feasibility/probe-summary.json).

RPKI's 367,204 union tuples are not 5 million independent authorizations. Its
daily unique set ranges from 360,226 to 362,599 tuples in this window. The raw
observation ledger preserves stability evidence, while a delta representation
can avoid repeated analytical values. DNS two-day common-group changes are
2.8237% (4,091/144,883), not a per-domain authoritative update rate. RIS fingerprints
repeat but cannot be removed merely because values look unchanged.

### Sensitivity, processing and transfer

For planning stress only, apply **0.5–3× nominal volume** to unmeasured longer
windows. This deliberately broad assumption is not inferred from one hour or
two days. RIS 24-hour logical MRT becomes approximately **5.12–30.75 GB** and
events **56.70–340.21 million** under that assumption. Do not schedule that day
until a smaller event path is measured. RPKI/DNS future gaps/schema changes also
invalidate same-density arithmetic. No extrapolated window was downloaded.

Measured cached RIS parsing took 98.7253 s for the hour using the Phase 13 Python
parser. ×24 gives 2,369.4079 s (39.49 min) **only under identical per-input cost**;
0.5–3× cost sensitivity gives 19.75–118.47 min, excluding downloads, encoding,
Kafka, Spark, sinks and queries. DNS two-day parsing took 7.3071 s; ×3.5 is
25.5748 s, with the same caveat. RPKI's 101.3213 s included fetching/parsing and
does not isolate parse throughput. New Spark end-to-end time is **unknown**.
For a chosen target rate q, publisher lower-bound time is accepted occurrences/q;
drain/startup/failure can add arbitrarily more. Source-time velocity is not q.

Required network input is the physical archive column, **unless reused from the
existing cache**, in which case no new source transfer is required. Transfer time
is bytes/effective bandwidth plus request/decompression overhead; actual bandwidth
and rate limits are not measured here. Reuse pinned artifacts, bounded allowlisted
requests, at most two simultaneous downloads, interruption/resume and checksums.
Do not download extra days merely to make a larger number.

### Storage amplification and disk guard

Normalized event encoding and sink sizes are unmeasured. A planning-only model
uses **200–800 serialized bytes/prefix occurrence** and **60–160 normalized
Parquet bytes/occurrence**. Both ranges are assumptions to be replaced after a
five-minute pilot; long AS paths and rich attributes can exceed them.

For the measured hour: serialized payload 0.945–3.780 GB; analytical Parquet
0.284–0.756 GB. A conservative staging model retaining compressed input,
decompressed MRT, Kafka payload and a second raw lake payload copy, analytical
events, plus 2 GiB scratch yields **4.82–10.97 GB peak disk**. Formula:
`73,771,296 + 427,068,595 + 2×4,725,156×encoded_bytes + 4,725,156×parquet_bytes + 2×2^30`.
This is a budget model, not a measured compression/amplification result. Streaming
decompression can remove staging; Kafka compression/retention can change it;
replication, temporary files, attributes and failure retries can increase it.

For a nominal 24-hour input, using 8 GiB scratch, the same model yields
**72.78–220.20 GB** before volume/cost sensitivity. Threefold upper density
can exceed the allowed disk budget. Thus **24 hours is not an MVP commitment**.
Process and measure one hour first; retain only declared runs and authoritative
outputs under a cleanup policy. Do not hold all event identities in Python RAM.

For RPKI, transport bytes/tuple and analytical Parquet bytes are unknown. CSV
expansion to JSON may amplify; canonical projection may reduce. Reserve 15 GiB
per bounded run initially and replace this allocation with measured source,
Kafka, raw/normalized/derived Parquet, checkpoint, shuffle and orphan bytes before
running repeat experiments. DNS output/storage amplification is also unknown and
is **not budgeted for implementation**; its existing physical input size is not
a prediction for a future typed sink. There is no use of the Phase 10 Parquet/raw
ratio to predict a different workload.

Review host: Phase 13 recorded Apple M3 Pro, arm64, 12 logical cores,
19,327,352,832 bytes RAM; [environment evidence](../workloads/results/dataset-feasibility/environment.json).
Review `df -k .` recorded 660,472,020 available KiB = **676,323,348,480 bytes**.
This does not measure container limits; inspect them before future service runs.
Preserve **20 GiB free**, consume less than half initial free disk, and impose an
initial **50 GiB total new experiment budget** with **15 GiB per-run reservation**.
The tighter limit wins. These are safety thresholds, not anticipated data size.
Never delete existing evidence/volumes to fit the experiment without approval.

Likely bottlenecks: MRT Python expansion/serialization before Spark; Kafka broker
I/O; repeated raw and processed writes; Parquet small files and manifest listing;
state-set joins/shuffles; JVM/Python/container contention on 18 GiB RAM. RIS MVP
needs no version buffer or full route-state store. RPKI daily joins are bounded
by complete published inputs, not a single giant stateful Python object. Retain
source inputs once, not a full MRT payload for every expanded prefix.

## Table D — selected-scope work packages

Assumptions: one developer/agent, existing local Kafka/Spark/MinIO infrastructure,
no paid cloud, no production operations, stable pinned dependencies. Hours include
coding, package-specific offline tests, debugging and integration preparation.
P8 adds cross-service/failure testing, P10 experiments, P12 final regression; these
are distinct from each package's local tests. Long unattended runtime is not
person-hours, but requires scheduling and resource supervision.

| Package | Dependencies | Best / expected / worst hours | Priority | Can postpone? / main difficulty |
|---|---|---:|---|---|
| P0 scope/legal approval | — | 4 / 8 / 16 | Gate | No; uncertainty can stop scope regardless of hours |
| P1 typed contracts + offline identities/oracles | P0 | 6 / 10 / 18 | Essential | No; avoid universal-schema errors |
| P2 additive lake schema/publication/reader registration | P1 | 8 / 14 / 24 | Essential | No; preserve configuration compatibility and immutable commits |
| P3 RPKI source import + completeness metadata | P1 | 6 / 10 / 18 | Essential | No; dates/regimes/count/hash barriers |
| P4 RPKI transitions + observed-state reconstruction | P2,P3 | 8 / 16 / 28 | Essential | No; gaps, late days, alternate layouts |
| P5 RIS expansion + source-coordinate oracle | P1 | 10 / 18 / 32 | Essential | No; highest parser/identity uncertainty, 57 errors |
| P6 RIS native occurrence ingestion + commit | P2,P5 | 8 / 16 / 28 | Essential | No; retries without suppressing legitimate repeats |
| P7 typed queries + simple batch/storage baselines | P4,P6 | 12 / 20 / 36 | Essential | No; fair units/layout/caching and meaningful reconstruction |
| P8 restart/partial-failure service validation | P4,P6 | 12 / 20 / 36 | Essential | No; common preservation claim depends on failures |
| P9 event-aware benchmark accounting | P6 | 8 / 14 / 24 | Essential | No; prefix occurrences != messages != branch rows |
| P10 repeated bounded experiments + negative results | P7,P8,P9 | 12 / 22 / 40 | Essential | No; avoid throughput-only demo |
| P11 academic report/reproducible evidence bundle | P10 | 10 / 16 / 28 | Essential | No; instructor-facing claim/evidence alignment |
| P12 final regression/CI/packaging | P11 | 6 / 10 / 18 | Essential | No; existing behavior remains protected |
| DNS additional adapter/state/privacy/tests | Successful selected MVP | 36 / 64 / 112 additional | Optional | **Yes**; not necessary to answer chosen RQs |
| Four-domain cross-evaluation | DNS + MVP | 12 / 22 / 40 additional | Optional | **Yes**; greatest incoherence risk |

Sum P0–P12: **110 / 194 / 346 person-hours**. These are engineering judgments,
not calibrated probabilities. The worst-case budget includes rework, not a promise
of an upper bound if access/academic gates fail. Full routing reconstruction,
ROV/hijack analysis, API/search changes, dashboards and multi-worker deployment
are unbudgeted new scopes and must not be hidden in adapters.

Dependency graph (paper only): P0 → P1; P1 → {P2,P3,P5}; {P2,P3} → P4;
{P2,P5} → P6; {P4,P6} → {P7,P8}; P6 → P9;
{P7,P8,P9} → P10 → P11 → P12.
The expected dependency critical path is P0→P1→P5→P6→P7 **or** P8→P10→P11→P12,
**120 hours with unlimited independent staffing**. With one developer, all labor
still totals **194 expected hours**; parallel agents do not eliminate integration
or operator attention. External approval delays are not included.

Conditional schedules (no invented course deadline):

| Work budget/week | Best 110 h | Expected 194 h | Worst 346 h |
|---|---:|---:|---:|
| 10 h | 11.0 weeks | 19.4 weeks | 34.6 weeks |
| 20 h | 5.5 weeks | 9.7 weeks | 17.3 weeks |
| 30 h | 3.7 weeks rounded | 6.5 weeks rounded | 11.5 weeks rounded |

If available effort is under roughly 110 hours, retain Phases 0–13 or explicitly
approve the state-only fallback. Do not promise the two-new-domain research
system by counting only parser/adapter coding.

## Minimum viable research systems

| MVP | Essential implementation/metrics | Research value / limitation | Best / expected / worst hours | Overrun probability judgment |
|---|---|---|---:|---|
| Two domains: Terraform reference + RPKI | Complete sets, gap barriers, reconstruction, full-vs-delta bytes/query/recovery | State-history comparison; no real high-velocity event path | 76 / 132 / 238 | Medium; academic velocity objection remains |
| **Two new domains: RPKI + RIS, preferred** | Typed observations/events, shared commits/recovery, exact set/multiset oracles, representation and batch/stream tests | Both information types and natural source-time velocity; no routing-state/causal join | **110 / 194 / 346** | Medium; primarily parser and integration risk |
| Three implemented domains: Terraform + RPKI + DNS | Existing Git plus two state adapters, qualified DNS comparison and legal gates | Measurement ambiguity control; no native event capacity RQ | 116 / 202 / 362 | Medium-high |
| Four evaluated domains with new DNS/RPKI/RIS paths | Preferred MVP + DNS + broader cross-tests | Maximum variety, weakest scope discipline | 158 / 280 / 498 | High |

Adding Terraform as an **unchanged reference** to the preferred MVP does not add
an implementation domain or justify claiming four-domain support. MVP must remain
smaller than the full vision.

## Future evaluation protocol and fair baselines

1. Freeze exact Phase 13 input manifests/hashes and Phase 12 reference. Generate
   per-artifact coordinate/digest oracles; own tiny fixtures for gaps, same-time
   repeats, late observations and before/after-publication failures.
2. Compare RPKI representations **within RPKI**: projected append-only daily
   Parquet, baseline+transitions, seven-day analytical checkpoints. Keep original
   compressed source and identical projected fields/codec/file-size goals in all
   variants; account raw and analytical storage separately. Reconstruct each
   observed date, query ASN changes and count correlated tuple changes. Test both
   warm and cold reads, three repetitions, median and range rather than false CI.
3. Compare RIS **within RIS**: streaming ingestion versus a bounded simple parser
   + batch Parquet/SQL analysis; optional Spark batch as an engine-controlled
   baseline. Same parser/counting policy and payload projection; compute per-minute
   announce/withdraw, top peer/prefix activity and declared bursts. No classifier
   claiming incidents. Record parser CPU, producer encode time, input bytes,
   source occurrences, broker offsets, batch duration/RSS/state and output files.
4. Try nested actual 5/15/60-minute inputs. Physical replay targets 0.5×/1×/2×
   the measured hourly mean test capacity; distinguish them from source-minute
   arrival shape and source-time rates. Randomize repeat order and record host/
   container limits, checkpoints, cache temperature and failed/saturated runs.
5. Reuse the existing Terraform path only as correctness/security regression and
   prior reference; different years/files/payloads mean no matched speed comparison
   against RIS. No summation of configuration comparisons and prefix events.

Acceptance priorities: exact logical identities/multiplicity and gap handling
first; storage/query costs second; capacity third. Aggregate counts alone cannot
prove correctness. Record missing/unexpected IDs, original error exclusions,
final/peak lag, useful canonical analytical history per byte (domain-specific),
file/manifest overhead and failed runs. An empty anomaly/alert result is valid.

If simple batch wins for daily states, say so. If streaming cannot sustain the
measured hour's mean on this machine, report saturation rather than enlarging
input to hide it. If delta storage saves no bytes at the measured scale, reject
that optimization. None of these negative results invalidate a correctly framed
comparative evaluation; they do invalidate unsupported superiority claims.
