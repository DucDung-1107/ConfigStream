# Phase 14 — research vision and investment review

Review date: 2026-10-12. Inspected baseline: `8026ae1cd1a0d7df1b2b6927000cfa8a776b022a`,
branch `main`, initially clean. This is a **decision review**, not an implemented
multi-domain system. The [decision](phase14-go-no-go.md) is **GO — REDUCED SCOPE**:
two new principal workloads, RPKI and RIS; existing Terraform history remains a
completed reference, not a third implementation workstream. DNS is postponed.
Instructor acceptance and a realistic time budget remain prerequisites.

## One problem, not four demonstrations

**Central problem:** how can a replayable historical data store preserve the
different information in repeated network-policy states and operational event
occurrences, while making the correctness, storage and query costs explicit?

The target users are network-measurement researchers and audit analysts who need
to reproduce what a source reported at a historical time. Their problem is not
deploying Terraform or detecting incidents automatically. A daily authorization
set is highly repetitive; a routing event ledger has meaningful multiplicity.
Discarding both through one content-deduplication rule loses evidence. Retaining
everything without completeness/provenance metadata makes reconstruction unreliable.

This can be **one** student systems-evaluation project if both workloads implement
the same source-occurrence ledger, immutable publication/recovery contract, and
auditable correctness/measurement protocol, with typed domain logic. It becomes
four unrelated demos if the only commonality is a Kafka broker or if success is
merely a total row count. No cross-domain causal inference or universal diff is
needed for coherence. Conversely, the evaluation must actually demonstrate these
shared guarantees; a conceptual envelope alone is not a contribution.

For the currently bounded RPKI/Git samples, scripts or a small analytical database
are adequate. Kafka is useful for replay/backpressure/recovery experiments, not
required by daily policy publication. Spark/object storage become plausible for
retained, partitioned histories and high-volume routing logs; their benefit must
be tested against simple batch baselines rather than asserted from dataset size.

## Evidence anchors: measured, not new Phase 14 experiments

All values below are from committed Phase 12/13 artifacts. Their original URLs,
hashes, scopes, filters and failures remain authoritative. No raw corpus was
downloaded, service started or end-to-end experiment rerun during this review.

| Evidence | Actual unit and window | Observations | Changes/events | Source-volume definition |
|---|---|---:|---:|---|
| [Terraform sample](../workloads/results/terrads/history-summary.json) | 375 accepted repositories; 6,505 file incarnations | 10,188 Git revisions | 3,683 adjacent file comparisons | 12,278,085 UTF-8 content bytes |
| [RPKI](../workloads/results/dataset-feasibility/rpki.json) | `ripencc.tal`, 2026-09-01–14 | 5,062,923 daily tuple rows | 12,016 added/removed tuples across 13 boundaries | 909,372,097 uncompressed CSV bytes; 63,816,988 compressed bytes |
| [DNS](../workloads/results/dataset-feasibility/dns.json) | Two complete `.li` days, 2026-09-01–02 | 2,234,863 measurement rows | 4,091 changed common A/AAAA groups out of 144,883 | 252,307,526 uncompressed Parquet column bytes; 98,828,670 physical bytes |
| [RIS](../workloads/results/dataset-feasibility/bgp.json) | `rrc00`, 2026-09-01 00:00–01:00 UTC | 2,300,578 MRT records | 4,725,156 parsed peer-prefix occurrences | 427,068,595 uncompressed MRT bytes; 73,771,296 compressed bytes |

Terraform's 77.48% single-version fraction and Phase 12 conclusion B remain
unchanged; the full 2,601,501,820-byte TerraDS static corpus is not temporal
history. See [Phase 12 acceptance](phase12-acceptance.md) and its
[temporal comparison](../workloads/results/terrads/temporal-comparison.csv).
The million-snapshot result remains controlled stress replay, not natural data.

RIS alone supplies measured natural event velocity: **1,312.5433 peer-prefix
events/source second**, and **181,868 events in the busiest source minute**.
There were 57 excluded parser errors and 118,172 repeated fingerprints, which
were retained. This is one collector-hour, not an Internet-wide rate or measured
ConfigStream capacity. RPKI and DNS provide daily observations, not comparable
online event rates. Repeated observations are valuable evidence of stability but
must not be called new changes. The types/byte definitions above must not be summed
into a single configuration-change total.

## Table A — competing scopes

Hours are judgmental best/expected/worst **person-hours**, including tests and
reporting, not measured durations or confidence intervals. The selected scope's
package accounting is in [feasibility](phase14-feasibility.md).

| Scope | Datasets and practical problem / users | RQs and academic value | Advantage | Limitation | Required change | Hours | Risk |
|---|---|---|---|---|---|---|---|
| A: preserve | Existing Terraform/FRR/NGINX; configuration audit researchers | Existing version-correctness and controlled capacity evaluation | Working system; least sunk-cost risk | Sparse natural history; Kafka/Spark not necessary at observed sample size | Final report and baseline analysis only | 16 / 28 / 48 | Low engineering; instructor-dependent academic risk |
| B: state history | Terraform + RPKI + DNS; observation-history analysts | RQ1 state branch, RQ2; reconstruct sampled states and study redundancy | Similar state representation; no MRT event path | No natural high velocity; DNS ambiguity; omits multiplicity test | Typed observations, completeness barriers, two canonicalizers, state queries | 116 / 202 / 362 | Medium-high; extra DNS legal/schema work |
| C: full heterogeneity | Terraform + RPKI + DNS + RIS; general infrastructure-history researchers | All RQs across three temporal categories | Broadest contrast and genuine routing velocity | Strongest four-demo/universal-schema risk; no causal common ground | State and event contracts, DNS processing, broader cross-tests | 158 / 280 / 498 | High |
| D: network specialization, selected | RPKI + RIS principal; Terraform reference only; network-history researchers | All three RQs on policy states versus routing occurrences | One network-history framing; strongest semantic contrast with two new sources | Not an operational ROV/hijack detector; one anchor/collector only | Narrow typed state and event paths plus shared publication/measurement | 110 / 194 / 346 | Medium, conditional on gates |
| Smaller state-only fallback | Existing Terraform + RPKI; historical audit researchers | RQ1 state branch and RQ2 only | Cheapest real new domain | Cannot answer RQ3 or natural-stream capacity | One state path and batch reconstruction | 76 / 132 / 238 | Medium academic; low-medium engineering |

These alternatives solve different problems. D does not replace Git, a validator,
or BGPStream. B is preferable if the instructor insists on state reconstruction
but does not require native event velocity. A is preferable if available effort
cannot fund the correctness and baseline work, not merely adapter demos.

## Table C — falsifiable research questions

### RQ1 — preservation under replay and recovery

**Question:** can a typed publication/identity contract preserve observed states
and event multiplicity across retries, disorder and missing observations?

**Hypothesis:** source-coordinate retry suppression plus explicit complete-state
boundaries gives identical committed logical outputs on clean and perturbed runs;
naive timestamp/content deduplication does not preserve all RIS occurrences.

Independent variables: delivery order, repeated delivery of the same coordinate,
different coordinates with identical payload/time, same timestamps, missing day,
and failure before/after lake publication. Dependent variables: missing/extra
identity multisets, daily set equality, invalid gap transitions, and replay hashes.
Datasets: RPKI/RIS; existing Terraform is a regression reference. Success means
**zero** unexplained missing/extra logical occurrences, exact reconstructed daily
sets, and no silent transitions across a missing/incomplete date. Failure is any
unexplained mismatch. Counterexamples belong in tiny owned fixtures, not changes
to the empirical source data. Real-artifact acceptance uses the Phase 13 counting
policy; errors are accounted for rather than concealed.

Confounders: source/parser revision, checkpoint reuse, cross-run duplicates,
overlapping/revised archive files and nondeterministic canonicalization. Feasible
only with a pinned input manifest and an independently checked offline oracle.
This tests source-occurrence preservation, **not end-to-end exactly-once** or
whether the archive observed every real-world event.

### RQ2 — redundancy versus reconstruction cost

**Question:** for comparable daily policy states, what storage/query trade-off
comes from full snapshots versus a baseline plus add/remove transitions?

**Hypothesis:** low change density reduces analytical Parquet bytes, at the expense
of reconstruction work. Compare full canonical daily sets, baseline+transitions,
and a seven-day analytical checkpoint variant on the same 14-day RPKI input.
Independent variables: representation, checkpoint interval, query date/window and
input length (nested actual 2/7/14-day windows). Dependent variables: analytical
and all-in bytes separately, file/manifest counts, exact set reconstruction,
query wall time and peak memory. Minimum useful result is a correctly measured
trade-off, including a negative one. A provisional engineering preference for
delta storage requires at least **20% lower analytical bytes** and no more than
**2× median reconstruction time** for the predeclared query suite. These are
project decision thresholds, not universal or measured findings.

Every representation keeps the same immutable source archive; do not count its
removal as compression. Match columns, codec, partition/file-size target, caching
and resource limits; report any layout change separately. At 14 days, initial
360,226 tuples + 12,016 transitions = 372,242 analytical rows, versus 5,062,923
daily rows: **7.3523% by row count**, not a predicted byte ratio. Schema/metadata
overhead and sorting can reverse a byte-level conclusion. The population/window
is too narrow to assert long-term production compression.

### RQ3 — event-path cost and bounded capacity

**Question:** what cost does recoverable Kafka/Spark ingestion add over batch
parsing/Parquet for the same operational routing occurrences and queries?

**Hypothesis:** streaming buys resumability/bounded progress latency but adds CPU,
serialization and small-file overhead; capacity on this host is unknown.
Independent variables: real nested 5/15/60-minute windows, batch versus streaming,
and declared physical replay targets (0.5×/1×/2× the measured hourly mean).
Dependent variables: accepted **prefix occurrences/s**, final/peak Kafka lag,
micro-batch duration, RSS, committed bytes/files, parse failures and exact
per-minute announce/withdraw counts. Three repetitions with fixed resources and
randomized execution order; record startup, parse, publish, drain and query time
separately. No invented throughput forecast from Phase 10 configuration rows.

Correctness must match the event-coordinate multiset before performance is
compared. At the 1× target, a provisional capacity gate requires final lag zero,
no OOM/disk guard, and drain under 10% of publisher duration; otherwise report
the measured saturation and stop expansion. Faster rates are accelerated replay,
not natural source arrivals. Source-minute distributions describe bursts, not
actual arrival timing at the collector/API. Confounders include Python MRT
expansion, encode size, JVM warmup, Kafka retention and host/container contention.
Only local capacity is evaluated, not horizontal scalability.

### Rejected questions

- Snapshot-versus-event **accuracy for the same physical changes**: datasets do
  not observe the same entities/events, and daily states lose intraday detail.
- RPKI churn **causes** BGP withdrawals, or DNS changes **cause** incidents:
  missing controlled causal evidence and comparable coverage.
- Reconstruct complete BGP routing state: no RIB baseline in the measured hour;
  even RIB+updates would require session/peer/gap/ADD-PATH semantics beyond MVP.
- Generalize per-file deployment frequency or prove a new distributed algorithm:
  neither the observations nor local infrastructure support this.

## Related work: novelty limits, checked primary sources

Titles, author lists and years are verified from the linked publisher/author
sources on the review date. Findings are qualitative; their throughput/population
numbers are not ConfigStream results. This is a focused related-work check, not
a new dataset survey.

| Work / verified authors and year | Actual problem and finding | Relationship and remaining differentiation |
|---|---|---|
| [Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark](https://cs.stanford.edu/~matei/papers/2018/sigmod_structured_streaming.pdf), Michael Armbrust, Tathagata Das, Joseph Torres, Burak Yavuz, Shixiong Zhu, Reynold Xin, Ali Ghodsi, Ion Stoica, Matei Zaharia, 2018; DOI 10.1145/3183713.3190664 | Incremental relational streaming with batch/interactive integration; establishes the engine design | Sharing Spark batch/stream APIs is not novel; our proposed contribution is workload-specific correctness and empirical cost accounting |
| [The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing](https://research.google.com/pubs/pub43864.html), Tyler Akidau, Robert Bradshaw, Craig Chambers, Slava Chernyak, Rafael J. Fernández-Moctezuma, Reuven Lax, Sam McVeety, Daniel Mills, Frances Perry, Eric Schmidt, Sam Whittle, 2015 | Event time, windows and triggers expose correctness/latency/cost choices for unordered streams | Watermarks and typed time semantics are established; proposed experiments test failure boundaries on these archived workloads |
| [MillWheel: Fault-Tolerant Stream Processing at Internet Scale](https://research.google.com/pubs/pub41378.html), Tyler Akidau, Alex Balikov, Kaya Bekiroglu, Slava Chernyak, Josh Haberman, Reuven Lax, Sam McVeety, Daniel Mills, Paul Nordstrom, Sam Whittle, 2013 | Persistent keyed state and fault-tolerant flow; anomaly-detector case study | Infrastructure observability and shared stateful runtime are prior art; no claim of equivalent Google-scale guarantees |
| [Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf), Michael Armbrust et al., 2020; DOI 10.14778/3415478.3415560 | Transaction-log/table metadata addresses publication, updates and object-store analytical access | Our existing manifests are narrower: single-writer batch publication, not a replacement for ACID/time-travel tables; no new table format proposed |
| [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google.com/pubs/pub36632.html), Sergey Melnik, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva Shivakumar, Matt Tolton, Theo Vassilakis, 2010 | Columnar nested-data scanning and distributed aggregation support operational analysis | Columnar historical analytics are established; local Parquet query/layout experiments cannot claim Dremel-scale results |
| [BGPStream: A Software Framework for Live and Historical BGP Data Analysis](https://iris.polito.it/retrieve/e384c42f-3878-d4b2-e053-9f05fe0a1d67/bgpstream_ready.pdf), Chiara Orsini, Alistair King, Danilo Giordano, Vasileios Giotsas, Alberto Dainotti, 2016; DOI 10.1145/2987443.2987482 | Common access and processing of live/archived BGP measurements | BGP parsing/stream analytics and archive abstraction are not new; any value here must come from committed history/recovery and fair storage/cost evaluation, not another BGP viewer |
| [RPKI is Coming of Age: A Longitudinal Study of RPKI Deployment and Invalid Route Origins](https://rpki-study.github.io/), Taejoong Chung, Emile Aben, Tim Bruijnzeels, Balakrishnan Chandrasekaran, David Choffnes, Dave Levin, Bruce Maggs, Alan Mislove, Roland van Rijswijk-Deij, John Rula, Nick Sullivan, 2019 | Longitudinal ROA deployment/quality and invalid-origin analysis using RPKI plus BGP | RPKI+BGP is an established research pairing; we explicitly do not replicate its security taxonomy or claim that simply combining these datasets is novel |
| [On Measuring RPKI Relying Parties](https://reposit.haw-hamburg.de/handle/20.500.12738/10762), John Kristoff, Randy Bush, Chris Kanich, George Michaelson, Amreesh Phokeer, Thomas Schmidt, Matthias Wählisch, 2020; DOI 10.1145/3419394.3423622 | Longitudinal relying-party fetching behavior and inconsistencies | Supports the validation/observation caveat; our daily differences cannot be attributed directly to operator edits |
| [A High-Performance, Scalable Infrastructure for Large-Scale Active DNS Measurements](https://doi.org/10.1109/JSAC.2016.2558918), Roland van Rijswijk-Deij, Mattijs Jonker, Anna Sperotto, Aiko Pras, 2016 | Daily active DNS collection with bounded measurement burden and longitudinal case studies | Large-scale DNS observations are prior art; current two-day address comparison is neither authoritative-zone history nor novel DNS measurement |
| [A study of how Docker Compose is used to compose multi-component systems](https://sailresearch.github.io/sail-website/data/pdfs/EMSE2021_A_Study_of_How_Docker_Compose_is_Used_to_Compose_Multi-component_Systems.pdf), Md Hasan Ibrahim, Mohammed Sayagh, Ahmed E. Hassan, 2021 | Git evolution and option usage; reports infrequent revisions in its cohort | Supports sparse configuration-history plausibility; its cohort does not estimate TerraDS or production deployment rates |
| [TerraDS: A Dataset for Terraform HCL Programs, v2](https://zenodo.org/records/20339474), Christoph Bühler, David Spielmann, Roland Meier, Guido Salvaneschi, 2026 | Licensed public HCL population and metadata/source artifacts; static research dataset | Population and real syntax are existing dataset contributions; ConfigStream's filtered temporal sample is separately measured, not supplied as full history by TerraDS |

[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html), Martin Fowler,
2005, is a **practitioner pattern**, not a new academic algorithm: reconstruct
state from a complete baseline/log. Daily RPKI transitions reconstruct only
observed daily sets, not every intervening policy action. RIS occurrence logs
without a RIB do not satisfy complete state reconstruction. Those qualifications
are a central design constraint, not cosmetic terminology.

Current state/checkpoint behavior must follow the pinned
[Spark 4.0.2 state guide](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-transform-with-state.html)
and [streaming recovery guide](https://spark.apache.org/docs/4.0.2/streaming/apis-on-dataframes-and-datasets.html),
not an unsupported generic exactly-once claim.

**Contribution type:** modest systems engineering plus reproducible empirical
evaluation: source-occurrence correctness, explicit completeness/gap handling,
within-workload retention/query trade-offs, and measured local event-path cost.
No claim to invent infrastructure observability, event sourcing, streaming,
network validation, temporal lakes or configuration versioning. Publication-level
novelty is unestablished; student-project value is conditional on experiments
that reveal trade-offs/failures rather than only demonstrating tools.

## Claim–evidence / adversarial writing check

| Claim | Evidence | Status |
|---|---|---|
| Original temporal configuration evidence is sparse | Phase 12 acceptance and committed sample | Supported; preserve conclusion B |
| RIS supplies real natural event velocity | One pinned collector-hour, source timestamps | Supported only for that scope |
| RPKI offers redundant policy-state history | 14 sets and exact tuple comparisons | Supported; not operator-command history |
| One shared preservation problem is coherent | Contract and test design in architecture review | Design argument; instructor approval needed |
| Shared implementation will improve costs/correctness | RQ1–3 proposed experiments | Needs future experiments; not claimed achieved |

Self-review: contribution is evaluation, not a new algorithm; terminology preserves
states versus occurrences; strong simple baselines are mandatory; negative storage
or capacity results are publishable; method scope excludes unsupported causal
joins/full routing reconstruction. The unresolved questions are instructor criteria,
raw/derived redistribution permissions, and actual event-path capacity.
