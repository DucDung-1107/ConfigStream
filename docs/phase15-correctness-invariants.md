# Phase 15 — correctness invariants and independent oracles

This is a **test specification**, not a claim that new-domain tests already pass.
[Contracts](phase15-semantic-contracts.md) specify identities;
[experiments](phase15-experimental-design.md) specify runs and thresholds.

## Declared failure and source model

Inputs are a frozen allowlisted artifact collection and pinned extraction policies.
Delivery can repeat, reorder, delay or stop; processes can fail before/after file
write, batch manifest, observation completion or Spark checkpoint acknowledgment.
Tests include fresh and retained checkpoints. No guarantee covers unavailable
Kafka retention, arbitrary concurrent writers, malicious source corruption,
unverifiable historical coverage or automatic incompatible-state migration.
Source exclusions are explicit and fixed; changing them is a method amendment.

Logical IDs are a **set of source-occurrence identities**; payloads form a multiset.
Distinct coordinates with equal values remain separate occurrences. Physical
Kafka deliveries and physical Parquet rows can exceed logical counts. Validate
full ID-to-payload maps/multiplicity, not just count equality. Checksum equality
is a compact check backed by full identity comparison on mismatch, not proof of
real-world truth. Hash collisions/conflicting payloads fail closed.

## Table C — hard invariants

| ID / applicability | Exact expected behavior | Real-data oracle | Negative owned fixture / counterexample | Failure policy / existing enforcement |
|---|---|---|---|---|
| I1 Provenance, all | Each logical record resolves to verified artifact(s), coordinate, policies and scope | Manifest/ledger join has no unresolved references | Record with wrong hash or absent manifest | Reject/quarantine run; Phase 12 sidecars partial reference, no general header today |
| I2 Replay identity, all | Repeated delivery of the same accepted source ID adds no logical ID; identical content/identity conflict cannot overwrite | Clean versus perturbed ID→payload maps | Same source coordinate delivered twice; then same ID altered payload | Conflict fails; existing configuration dedup is watermark-bounded, not global replay suppression |
| I3 Multiplicity, RIS and raw DNS | Distinct source coordinates survive equal payload/time; canonical state reuse preserves observation references | Coordinate ledger and payload multiplicities | Two identical-looking routing events at different offsets; equal DNS rows with separate coordinates | Any lost occurrence fails; old snapshot dedup is unsuitable for payload/event reuse |
| I4 Complete/comparable states, RPKI; qualified DNS | Normal transitions require complete comparable observations; partition completeness never implies missing DNS entity is absent | Daily set digests and eligibility matrix; DNS qualification ledger | Drop final chunk/day; timeout or missing DNS response; known scope change | Pending/quarantine or new segment baseline, no false removals; no existing complete-state barrier |
| I5 Temporal honesty, all | Original time/precision retained; ordinals/order not invented causal time; unknown stays unknown | Source times and policy descriptors | Tied Git times, inverted commit times, same-time RIS, DNS millisecond encoding, absent validator instant | Bad conversion rejects; uncertain ordering annotated; old UTC validation only partially covers this |
| I6 Committed visibility, all | Readers use frozen published manifests; uncommitted batch and incomplete observation invisible to derived-history queries | Visible path/manifest inventory before/after failures | Kill after first output file or after manifest but before checkpoint | Retry uncommitted batch, preserve committed batch; existing lake enforces batch-level convention, not day barrier |
| I7 Exact reconstruction, Terraform/RPKI/DNS projections; RIS occurrence projection | Reconstructed accepted projection equals oracle; raw and normalized Terraform remain distinct | Raw blob hashes, daily sets, qualified DNS groups; RIS ID→attributes map | Missing delta, wrong checkpoint, reused DNS state losing time/quality, event aggregation substituted for log | Fail representation/claim; no complete BGP world-state or DNS zone reconstruction asserted |
| I8 Error accounting, all | Every input unit accepted, deliberately out-of-scope or excluded with safe category/coordinate; never silent | Artifact ledger partition and totals by reason | Unsupported MRT/ADD-PATH, malformed row, truncated artifact | Truncation blocks completeness; known body exclusions counted; original 57 retained, no conversion into valid events |
| I9 Existing compatibility, original configuration system | Old schema/IDs/text-v1/defaults/checkpoints/manifests/alerts keep meaning | Existing unit/API/Spark/service suites plus Phase 12 acceptance oracle | New registry accepts old path with incompatible schema | Stop additive change/revert it; old regressions already exercise original behavior, not new-domain compatibility |

All nine are non-negotiable; passing a cost target never compensates for failure.
I3 is not content uniqueness. I4 completeness is relative to a declared source
scope, not absolute Internet completeness. I7 is only the named projection.

## Independent oracle policy

**Terraform:** reuse Phase 12 accepted source/version manifest, raw hashes and
ground-truth adjacency edge set: 10,188 accepted IDs and 3,683 edges over 6,505
incarnations. Exact normalized diff output is a reference regression using the
existing text-v1 logic, **not independent semantic HCL truth**. Validate source
bytes/edge identities independently of the streaming processor. Preserve its
license, whole-incarnation secret filtering, timestamp and censoring policies.
See [Phase 12 acceptance](phase12-acceptance.md) and
[edge ledger](../workloads/results/terrads/ground-truth-edges.csv).

**RPKI:** a separate small reference calculation reads frozen CSV artifacts,
validates numeric/network fields, builds canonical sets and sorted checksums,
and computes differences without calling the future Spark transition transform.
Cross-check an SQL anti-join implementation on the same projection and inspect
owned duplicate/maxLength/family fixtures. Acceptance: exact 14 sets and 13
eligible consecutive comparisons, 5,062,923 total original observations,
12,016 transition tuples; complete-artifact checksums and zero missing dates in
the selected source window. Future source-invalid rows must block a complete
state unless an amended, explicitly qualified policy is justified; they cannot
quietly become removals.

**DNS:** an independently expressed SQL/grouping reference validates all source
row coordinates/timestamps/status counts, canonical IP sets and comparisons.
The unchanged Phase 13 policy must reproduce 144,883 common groups, 4,091 changed,
7,716 added values and 7,612 removed values, while 485 newly observed and 676
no-longer-observed groups stay **observation availability**, not deletion events.
Use owned missing/timeout/NODATA/CNAME/varying-worker fixtures to validate
qualifications. DNS root-cause ground truth does not exist here; causal accuracy
and authoritative-zone reconstruction are N/A. An independent SQL expression
reduces transform coupling but still shares the policy's assumptions.

**RIS:** build a header-framing/source-coordinate ledger independently of Spark
ingestion. Every framed MRT record gets a coordinate, including 57 excluded body
parse records under the pinned reference parser/policy. All-record indices and
byte ranges must be continuous and end at verified decompressed EOF. Accepted
unicast expansions must match 4,725,156 occurrences (4,462,391 announcements,
262,765 withdrawals) for the hour reference, without removing 118,172 equal-looking
fingerprints. Recount per artifact/action/time and compare full coordinate payloads.
Manually/independently decode owned examples for IPv4/IPv6, MP_REACH/UNREACH,
ADD-PATH, malformed and unsupported types. A header ledger independently verifies
occurrence framing but **not every semantic parse**. Sharing mrtparse with the
importer gives conditional parser correctness only; second-parser sample checks
or fixture/manual checks must document disagreement and coverage, never imply
universal semantic validation. No uncontrolled new parser install in Phase 15.

Oracles may share the formal canonicalization specification but must not call
the pipeline transformation they validate. Record oracle code/version, input
manifest, scope and limitations. Phase 13 aggregate summaries alone are
insufficient exact-ID oracles. Establish ledgers before throughput experiments.

## Perturbation and recovery acceptance suite

For each typed kind run clean, retry, reverse/seeded-shuffle, same-time,
late-completing input and safe failure-placement cases. Own fixtures contain
finite sequences with explicit expected identities; seeded disorder is separate
from unchanged empirical source history. Remove or corrupt a descriptor/chunk,
not a hidden favorable resample. Sources that lack a completeness claim must
remain qualified after recovery.

Late records must be preserved in the immutable input/evidence ledger even when
outside an aggregation watermark. For bounded historical acceptance, use declared
replay settings/available-now and coverage barriers; do not apply the old ten-minute
snapshot watermark to years of Git history or daily states. Late completion after
a missing interval can generate a **new validated derived run**, never silently
mutate a previously published transition chain. Record supersession/lineage and
freeze reader inputs. Exhausted retention is visible irrecoverable failure, not
an empty successful replay.

Same-checkpoint recovery tests include commit-before-checkpoint and unpublished
partial files. Fresh-checkpoint replay creates physical output namespaces; an
analytical logical view must reconcile source IDs across selected runs or select
one run manifest. This global reconciliation must be explicitly implemented and
costed; the current watermark-bounded dedup does not supply it.

## Actual guarantees, not fictional transactions

Inspection of [lake writer/reader](../spark/streaming/sinks/lake.py) and
[conditional manifest publication](../lake/client.py) shows deterministic
query UUID/batch namespaces, overwrite only for uncommitted batches, a conditional
manifest put after output writes, and reads restricted to declared committed
paths. On replay, existing commit format/IDs/counts are checked, not complete
payload cryptographic equality. New-domain completion descriptors and logical
ID/payload validation are required additions, not current guarantees.

MinIO publication, Spark checkpoint, Kafka alerts and Elasticsearch are not one
atomic transaction. Kafka retries may duplicate physical messages; derived search
is rebuildable. No multiwriter serialization or universal ACID/time-travel lake
is claimed. [Spark's pinned Kafka guide](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html)
also requires care about sink duplication. RQ1 concerns the explicitly validated
committed **logical projection**, not end-to-end exactly-once delivery.
