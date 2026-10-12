# Phase 15 — bounded expansion, effort and legal gates

No downloads, service starts or new-domain implementation occur in this phase.
Inputs and arithmetic reference
[Phase 14 estimates](../workloads/results/phase14/feasibility-estimates.json)
and the original [Phase 13 manifest](../workloads/results/dataset-feasibility/probe-manifest.json).
Planning numbers below are **not future observations or measured runtimes**.

## Initial resource envelope and scenarios

Recorded Phase 13 host: M3 Pro, arm64, 12 logical cores, 19,327,352,832 RAM
bytes (18 GiB), Python 3.12.14. Phase 15 `df -k .` reported 660,408,724
available KiB (676,258,533,376 bytes). This is host disk, not Docker's allocation;
recheck Docker limits before any future run. Spark 4.0.2/Java 21 and Kafka 4.1.2
are the existing pinned runtime, not new adapter performance measurements.

| Window | Input compressed/physical bytes | Logical source representation | Records | Status |
|---|---:|---:|---:|---|
| Terraform accepted sample | Archive-cache bytes not comparable to history UTF-8 | 12,278,085 UTF-8 bytes | 10,188 versions | Measured, reuse |
| RPKI 14 days | 63,816,988 XZ | 909,372,097 CSV | 5,062,923 observations; 12,016 transition tuples | Measured |
| RPKI 30 days | 136,750,689 nominal | 1,948,654,494 nominal CSV | 10,849,121 nominal observations; actual future transitions unknown | Scenario: volume ×30/14, rounded; not forecast |
| DNS 2 days | 98,828,670 Parquet | 252,307,526 uncompressed columns | 2,234,863 rows; 4,091 changed common groups | Measured; columns ≠ CSV/MRT/text bytes |
| DNS 7 days | 345,900,345 nominal | 883,076,341 nominal columns | 7,822,021 nominal rows; changes unknown | Scenario: volume ×7/2 |
| RIS 1 hour | 73,771,296 Gzip | 427,068,595 MRT | 4,725,156 occurrences | Measured |
| RIS 24 hours | 1,770,511,104 nominal | 10,249,646,280 nominal MRT | 113,403,744 nominal occurrences | Scenario: ×24, **not approved** initial download |

Use arbitrary 0.5–3× nominal volume as a **planning sensitivity range**, not CI
or empirically measured diurnal variation. RIS same-cost cached parser scenario
is 98.73 s/hour, ×24=2,369.41 s; it does not predict streaming runtime. RPKI's
101.32 s includes fetch+parse; DNS's 7.31 s is cached probe processing. Baseline
engine, Kafka serialization, new state/publication and query costs are unknown.

Preserve ≥20 GiB free and consume ≤50% of initially free disk; cap new experiment
cache/output at 50 GiB and initially reserve 15 GiB/run. Max two simultaneous
downloads, one measured job, aggregate measured RSS budget 12 GiB with actual
container limits recorded. No paid cloud. Stop before unsafe reserve/RSS pressure.
Source bytes alone understate disk because Kafka, raw/normalized copies,
checkpoints and scratch coexist.

For RIS planning only, assume 200–800 serialized bytes/occurrence, 60–160
normalized Parquet bytes/occurrence; **these are unmeasured assumptions**.
Peak estimate = compressed+decompressed staging+2×events×serialized size
(Kafka and raw lake)+events×Parquet size+scratch. With 2 GiB scratch, the hour
scenario is **4.82–10.96 GB decimal**; with 8 GiB scratch the day is
**72.78–220.20 GB** before density sensitivity or additional representations.
The 24-hour case violates the initial 50 GiB cap. Stream-decompression/raw archive
references may reduce footprint, but only measured byte accounting can authorize
that change. RPKI/DNS transport amplification and state RSS are unknown; measure
before seven/30-day expansion. No capacity predicted from Phase 10's snapshot rate.

## Gated dataset expansion

| Source / progression | Required closure | New research value / stop |
|---|---|---|
| Terraform: existing 375 accepted repos only | Old oracle/license/security policy still reproducible | Reference ordinal version test; no full-population remine without new question |
| RPKI: pinned 14 days → 30 consecutive days → optional different anchor/longer segment | Per-day integrity/schema/regime accounting; exact sets and all eligible transitions; measured encoding/RSS/storage within guards; access/terms | Check checkpoint-chain sensitivity and additional complete boundaries; stop gap ambiguity, mismatch, resource breach; no replacement dates |
| RIS: pinned 5 → 15 → 60 minutes | Framing/expansion ledger, multiplicity, 57-error hour audit, actual transport/lag/recovery and disk/RSS compliance | Startup/burst/capacity sensitivity; stop any unexplained ID loss or 1× drain failure before larger windows |
| RIS: multiple hours / second collector; 24h only separate approval | Hour passes; per-source hashes/parser coverage; new diurnal/peer question and independently budgeted disk, legal access | Test in-domain variability, not global extrapolation; 24h excluded from initial cap |
| DNS: pinned 2 days → 7 days → optional scope extension | E4 generalization and independent oracle; qualified empty/missing handling, terms/publication approval, actual resource estimate | Longer repeated-state chains and qualification sensitivity; stop if only changed-IP counts or causal labels remain |

Every expansion freezes exact dates/artifact selection **before** collection and
retains missing/corrupt statuses. No larger window solely to inflate counts.
Private cached raw sources are reused; no continuous live collector, authentication
bypass, broad crawl or raw-data Git commit. DNS may be postponed without invalidating
RPKI/RIS results, but the four-role K3 claim then remains untested and must be revised.

## Table H — work packages and dependency graph

Judgmental **person-hours**: best / expected / worst, not observed development
time or probabilistic confidence bounds. One developer/agent, local existing
Kafka/Spark/MinIO, no paid service/production operations, no invented deadline.
Testing/debugging is included in package estimates, not silently postponed.

| Package | Dependencies | Hours | Deliverable / acceptance | Stop / postpone |
|---|---|---:|---|---|
| W0 approval/access/budget | None | 4 / 8 / 16 | Instructor accepts problem, local evaluation, negative findings and private-source policy | Global approval blocker; mandatory |
| W1 contracts + early DNS/RIS oracle feasibility | W0 | 12 / 20 / 36 | Offline identity/completeness/time fixtures; MRT offset proof; DNS empty/unknown policy audit | No legal/identity solution: stop before services; mandatory |
| W2 Terraform reuse audit | W1 | 4 / 6 / 12 | Existing accepted IDs/edges unchanged | Regression mismatch blocks compatibility; mandatory |
| W3 additive typed publication/read registration | W1 | 10 / 18 / 32 | Old manifests stable; completion descriptors not partial visible states | Break old history or need new transaction engine: stop; mandatory |
| W4 RPKI import/coverage | W1 | 8 / 12 / 24 | Source/row/descriptors verified | Partial state accepted as complete: stop; mandatory |
| W5 RPKI transitions/reconstruction | W3,W4 | 8 / 16 / 28 | 14 sets/13 boundaries exact | Any set mismatch blocks costs; mandatory |
| W6 RIS framing/expansion oracle | W1 | 14 / 26 / 48 | Coordinate/error ledger and independently checked fixtures | Cannot explain parser exclusions/coordinates: stop event claims; mandatory |
| W7 RIS typed event path | W3,W6 | 8 / 16 / 28 | 5/15/60-min accepted ID projection and retries | Lost legitimate equal events: stop; mandatory |
| W8 DNS observations/status/canonicalization | W1 | 14 / 24 / 42 | Partition and group evidence separate, no inferred empty/deletion | Postpone if no qualified source model |
| W9 DNS generalization/reconstruction | W3,W5,W8 | 16 / 28 / 50 | E4 uncertainty and reuse tests with exact qualified projection | Postpone DNS/K3 claim if E4 fails |
| W10 cross-kind recovery/failure suite | W2,W5,W7,W9 | 16 / 28 / 50 | Same/fresh checkpoint, partial commit/observation failures | Cannot cut correctness to meet schedule; mandatory for retained kinds |
| W11 batch/query/storage baselines | W5,W7,W9 | 18 / 30 / 54 | Matched B0–B7 and registered queries; negative results retained | No meaningful same-input baseline: stop superiority claims |
| W12 occurrence-aware measurement harness | W7 | 10 / 18 / 32 | Units/lag/RSS/bytes/drain/failures counted | Cannot identify accepted denominator: no throughput claim |
| W13 bounded repeated experiments | W10,W11,W12 | 16 / 30 / 54 | Correctness first; repeated matched runs, measured expansion decisions | Resource/identity failure: report and stop enlargement |
| W14 academic synthesis | W13 | 12 / 20 / 36 | K1–K3 evidence/limits, causal and unit audit | Only four demos: reduce claim/scope |
| W15 regression/CI/reproduction | W14 | 10 / 18 / 32 | Old/new tests, safe results, green CI, reproduction checks | No completion claim before regression |

Totals: **180 / 318 / 574 person-hours**. Expected dependency critical path with
unlimited parallel staffing is **188 h**:
W0→W1→W3→W5→W9→W11→W13→W14→W15. One person's expected labor is **318 h**, not
188 h. This technical dependency lower bound excludes chronological milestone
approval barriers and waiting time; it is not the implementation schedule.
Phase 14's full vision was 158/280/498: this freeze raises expected effort
by 38 h for explicit oracle, DNS-qualification, baseline and regression work;
it does not make the broader plan cheaper merely because the owner requests it.
At 10/20/30 productive h/week, expected labor is 31.8/15.9/10.6 weeks, excluding
approval delays. No actual weekly budget is supplied; resource gate is conditional.
Dropping W8/W9 alone removes 30/52/92 h, but shared work must then be formally
re-budgeted; do not present this subtraction as the old 194-hour reduced MVP.
Longer windows, cluster, API and incident products are optional/unbudgeted.

## Legal/publication policy, not legal advice

| Source | Research/cache use and attribution | Redistribution / public Git/demo gate |
|---|---|---|
| TerraDS/Terraform | [TerraDS v2](https://zenodo.org/records/20339474) artifact CC-BY-4.0; pinned upstream license evidence and filtering in Phase 12 | Artifact license ≠ source-history/vendor licenses. Existing root-license acceptance is not every-old-file adjudication; no raw config redistribution |
| RIPE RPKI | [Archive description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md) and [copyright statement](https://www.ripe.net/about-us/legal/copyright-statement/) govern attributed research/educational use subject to conditions | Unrestricted raw/normalized redistribution not established; resolve applicable grants before publishing tuples; anonymous FTP ≠ authenticated repository agreement |
| RIPE RIS | [MRT documentation](https://www.ris.ripe.net/docs/mrt/), RIPE credit; [commercial conditions](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/commercial-use/) are not a blanket academic open-data license | Access/code license does not sublicense peer data; aggregate counts/hashes and owned fixtures only until source-data publication is cleared |
| OpenINTEL | [Terms, 2024-12-11](https://openintel.nl/download/terms/): CC BY-NC-SA 4.0, required publisher acknowledgment and scientific citation | Redistribution must retain full unmodified terms and appropriate attribution/share-alike/noncommercial conditions. This repository publishes no external rows/sets; exact publisher notice must accompany future applicable communications |

Credit: OpenINTEL, University of Twente, SIDN, NLnet Labs, SURF; applicable research
citation [van Rijswijk-Deij et al., 2016](https://doi.org/10.1109/JSAC.2016.2558918).
The required acknowledgment wording is linked in the publisher terms; the license
gate must attach that notice before future publication rather than replace it
with an invented sublicense. Phase 15 rechecked the OpenINTEL terms/dictionary;
the RIPE copyright direct web request failed, so preserve the Phase 14 primary
indexed verification and treat redistribution as unresolved, not permission.

Academic use, aggregate review, raw redistribution, normalized data publication
and public demos are separate permission questions. Unknown rights block raw
publication, not a reason to hide terms. Keep raw network identifiers, DNS TXT/
SOA/contact values, configuration contents and secrets out of Git/logs/API. Hashes,
source links and aggregate method/results do not grant source licenses. CI uses
only owned clearly labeled fixtures. External IaC is never executed. No source
permission contact is initiated without user approval.

## Representativeness limits

Public licensed Git samples cannot establish private enterprise or deployment
frequency. Single-anchor daily RPKI loses intraday ordering and may reflect
validator policy. Two `.li` days with sparse/variable DNS fields cannot represent
all DNS or resolve authoritative causality. One RIS hour/collector depends on
peers and bursts; parser exclusions restrict coverage. Repeated templates and
equal payloads affect storage but do not establish retry equivalence. Expanding
in-domain windows tests sensitivity, not global extrapolated totals.
