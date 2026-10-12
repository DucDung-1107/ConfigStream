# Phase 15 — paper semantic contracts

Contract proposal v1. No production schemas, serializers or jobs change.
[Method](phase15-research-methodology.md) defines the problem;
[invariants](phase15-correctness-invariants.md) define acceptance.

## Taxonomy and boundaries

**VersionedConfiguration** is source content in a declared ordinal sequence.
**StateObservation** is evidence of a scoped state at an observation point, not
necessarily of every real-world entity. **StateTransition** is derived between
complete comparable states, not a native operator event. **MeasurementObservation**
adds measurement conditions and unknown causality; a complete file does not make
every domain result complete. **EventOccurrence** records a particular occurrence;
payload equality does not define occurrence equality. **SourceProvenance** binds
all kinds to immutable evidence and policy. MeasurementObservation specializes
observation principles but has a separate typed payload/comparability contract.

## Minimal Temporal Evidence Envelope

This is a small common **header plus typed payload reference**, not one schema
with dozens of nullable domain fields. Proposed header requirements:

| Header field | Requirement / meaning |
|---|---|
| `envelope_version`, `record_kind`, `payload_schema_version` | Required explicit versions and one declared kind; unknown versions rejected |
| `source_id`, `dataset_id`, `source_scope` | Required publisher/source, frozen collection/window identity and scoped coverage descriptor ID |
| `logical_record_id` | Required hash of a kind-specific, versioned canonical identity, not payload hash alone |
| `provenance_manifest_id` | Required immutable manifest reference; binds artifact IDs/URLs/hashes, licenses, parser, canonicalization and counting policies |
| `source_coordinate` | Required tagged coordinate: Git blob/commit/path; CSV row/set descriptor; Parquet row-group/row; MRT byte-offset/expansion; derived input IDs plus operation |
| `time` | Required tagged descriptor: known point with unit/precision, interval/date scope, or explicit unknown with reason. Actual source timestamp stays in its typed role |
| `quality_status` | Required declared accepted/excluded/incomplete/incomparable, with safe category; does not imply a probability of correctness |

Artifact ID/hash and parser/canonicalizer/counting-policy versions are **required
manifest bindings**, not repeatedly copied into every row. Derived records bind
all input artifacts. A nullable policy version is replaced by explicit
`not_applicable`. Observation and source times are distinct typed fields only
where supplied; ingestion time/Kafka coordinates belong in the **delivery ledger**,
not stable identity. Preserve first/last arrival per delivery; retries naturally
change arrival time without changing source evidence. Do not stamp unknown source
time with ingestion time or midnight. No common version ordinal or universal entity
field is imposed on RIS.

Hashing uses SHA-256 of UTF-8 canonical JSON arrays with type-tagged fields,
explicit version, sorted object keys, no NaN and domain-defined string/IP/date
normalization; never ambiguous concatenation. Store the identity preimage/digest
privately for collision checks. Same ID with different identity/payload is a
**fatal conflict**, not first-wins silently. Policy changes create a new derivation
namespace and lineage; physical delivery IDs are separate. Identical artifacts
at duplicate URLs are one artifact. Revised bytes at one URL create a new artifact
revision; they do not silently replace published history. Cross-artifact semantic
overlap is not deduplicated unless publisher occurrence equivalence is established.

Backward compatibility is additive registration with explicit readers. Preserve
all v1.0 configuration schemas, serializer record union, existing paths/checkpoints,
text-v1 normalization and old commit readers. No automatic migration or acceptance
of unknown fields. Changing an identity/comparison rule requires a new policy ID,
frozen-input revalidation and recorded freeze amendment.

## Table B — typed identities and temporal contracts

| Kind / dataset | Identity | Time / ordering | Completeness | Deduplication | Reconstruction |
|---|---|---|---|---|---|
| VersionedConfiguration / Terraform | Existing snapshot ID; repository, path incarnation, selected Git commit and extraction ordinal sidecar | Git committer UTC/offset; **ordinal**, not timestamp, orders accepted contents | Accepted pinned first-parent file history, explicit censoring/exclusions | Same snapshot ID delivery; not equal content at nonadjacent versions | Accepted raw version and normalized text, not deployed infrastructure |
| StateObservation / RPKI | Scope+artifact+date+policies descriptor; membership tuple distinct from observation | Observation **day precision**; actual validation time unknown unless source supplies it | Separate complete-set descriptor, expected/parsed counts, digest, integrity and comparability | Same source row retry; canonical duplicate tuple collapsed in set with raw multiplicity audit | Exact validator-reported canonical daily set |
| StateTransition / RPKI | Previous/current observation IDs+tuple+add/remove+policy | Consecutive comparable days only; interval, no precise operation time | Both complete; missing/incomparable day forbids normal edge | Same derived identity | Baseline+deltas within contiguous segments, checked daily digests |
| MeasurementObservation / DNS | Source+partition artifact+row-group+row for raw; derived group ID additionally binds grouping/comparability policy | Actual worker timestamp; daily grouping retains contributing time range, not invented single measurement time | Partition complete and group evidence separately qualified | Retry by coordinate; equal values reused without erasing measurement occurrences | Exact declared observed projection, not authoritative zone |
| ObservedValueDifference / DNS | Both group observation IDs+comparison policy | Difference between comparable observations, unknown cause | Both present/successful under declared policy; no absent-result deletion | Derived identity only | Declared address sets with observation references/quality masks |
| EventOccurrence / RIS | Scope+artifact SHA+MRT coordinate+expansion ordinal+policy | Source time, original precision; ties remain multiple; no global causal total order | Accepted parsed subset plus exclusions ledger; archive coverage is not global BGP completeness | Same source occurrence delivery only | Occurrence ledger/projection; no routing-state reconstruction |

## Terraform: preserve the existing reference

Repository canonical ID and pinned end SHA, license evidence, file path and
incarnation, current/previous selected commit, raw SHA-256 and extracted ordinal
remain in the Phase 12 sidecar. Existing ConfigSnapshot carries UTC event/ingest
times, raw hash, positive ordinal and exactly one content/URI location. Accepted
raw contents and text-v1 normalized contents are distinct projections: normalization
is **not lossless raw reconstruction**, and text diff is not Terraform plan analysis.

Initial content is version 1, not an observed prior change. Deletion closes the
incarnation without a deletion snapshot; recreate is a new incarnation; rename
splits deletion/creation. Reversion retains its later ordinal even with equal raw
hash. Unchanged commits remain suppressed by the extractor. Ground-truth edges
are adjacent accepted ordinals; preserve committer-time ties/inversions instead
of repairing clocks. Secret/encoding/license rejection rules are unchanged.

## RPKI: independent completeness barrier

Verified CSV schema is `URI, ASN, IP Prefix, Max Length, Not Before, Not After`:
[original probe](../workloads/results/dataset-feasibility/rpki.json),
[publisher archive description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md).
Canonical membership is `(trust-anchor scope, integer ASN, canonical network
prefix, integer maxLength)` with family/prefix-length/maxLength validation.
URI, validity and validator details remain provenance/metadata, not authorization
identity. Retain source multiplicity separately even if a canonical set has duplicates.

A **StateDescriptor** exists independently of members: observation ID, scope,
date, validator/regime evidence (unknown explicitly if unavailable), complete
artifact inventory and hashes, expected rows, parsed/invalid/duplicate counts,
unique count, set checksum, policy versions, completeness and comparability flags.
Set checksum hashes sorted canonical tuple encodings with explicit framing;
empty-but-complete set is distinct from missing descriptor/input. Transport must
not equate EOF of one partial chunk with complete daily scope.

Rows may be published as auditable pending input, but a daily analytical state and
its transitions become visible only after completeness verification and immutable
completion publication. A stream batch commit alone is **not** this day barrier.
Comparability requires matching scope/schema/canonical policy and a declared
validation regime. Unavailable regime evidence is explicitly qualified as
`archive-reported, regime_unknown`, with conservative same-window comparison;
a known regime change starts a new baseline segment. Never claim equivalence of
validators from dates alone.

For complete consecutive comparable sets S,T: added=T−S, removed=S−T,
unchanged=S∩T. No edge across missing/incomplete/incomparable days. Next complete
state starts a new baseline segment; optional endpoint comparison across a gap
would be separately labeled and is excluded from primary transitions. Recovery
rechecks descriptors/digests before publication. Oracle: all 14 day sets,
13 eligible boundaries and 12,016 add/remove tuples under the original scope/policy.
These describe validated authorization results, not commands or intraday actions.

## DNS: evidence preserved, uncertainty not erased

Raw observations retain source `.li` partition, deterministic Parquet coordinates,
worker/query/response fields, status, time, address data and relevant collection
qualifiers in private storage. For the Phase 13 reference projection use successful
status-zero A/AAAA rows; group by lowercase/trailing-dot-stripped
`(query_name, query_type, response_name, response_type)`, canonical IP-value set.
Keep contributing row IDs, observation times/range, status counts and worker IDs
even if state values are stored once. A same-day group may mix observations; it
is a **daily measured projection**, not one atomic DNS response.

The [official dictionary](https://openintel.nl/data/dictionary/) specifies a
worker-recorded epoch-millisecond field with **second-level precision**, worker
identity and RCODE; 65535 indicates timeout. Workers sit behind caching resolvers.
Schema fields evolve and are sparse. Thus millisecond encoding must not imply
millisecond measurement accuracy; worker ID does not prove constant resolver or
vantage conditions. Known collection changes block primary comparison; unknown
conditions stay qualified, not falsely certified identical.

Four result distinctions are mandatory: (a) no measurement/unknown coverage;
(b) unsuccessful measurement; (c) successful empty answer **only with validated
query-level evidence**, not zero selected A/AAAA rows; (d) successful nonempty
address-set evidence. Current group extraction does not establish (c) for every
absent group. Do not invent an empty set, NXDOMAIN, NODATA or entity deletion from
absence. Preserve error/missing categories and flag unclassifiable emptiness.

Comparison eligibility controls record/query type, response status, partition
integrity, scope/method version, and available worker/time qualifiers. Phase 13's
4091 differences use the declared common-day grouping policy and do **not** assert
matched resolver/vantage. Future worker-stratified sensitivity analysis is separate
from that immutable reference result; mismatched/unknown conditions are reported,
not rewritten into favorable results. TTL/geolocation/signatures are not address
identity but may be retained as context, not used as causal labels.

Publish only **observed value difference, cause unknown**: data-values differ
under the policy, not necessarily authoritative DNS change. No fictitious confidence
probability. DNS tests whether a complete-artifact method can preserve *incomplete
entity knowledge*—a boundary absent from complete RPKI membership comparison.

## RIS: native occurrence identity and the unresolved parser gate

Proposed ID is SHA-256 of `(identity-version, collector/scope, artifact SHA-256,
decompressed MRT header byte offset, expansion ordinal, counting-policy version)`.
The source ledger also stores all-record index, record type/subtype/length,
source timestamp, peer ASN/IP and interface/context when available, AFI/SAFI,
action, canonical prefix, ADD-PATH path ID and ordered path/attribute projection.
Unknown session identity stays unknown; no inferred continuous session.

[RFC 6396](https://datatracker.ietf.org/doc/html/rfc6396) defines MRT header
length framing. Inspection of [mrtparse v2.2.0 Reader](https://raw.githubusercontent.com/t2mune/mrtparse/v2.2.0/mrtparse/__init__.py)
shows reads of a 12-byte header followed by the declared body length; it returns a
mutable Reader, not an occurrence-coordinate API. A counting stream or independent
header framer can record start offsets before decoding, but this is a **feasible
design hypothesis, not a tested adapter**. Phase 16 must prove it on owned fixtures,
including malformed records, before service work. Truncated framing blocks the
artifact; do not guess resynchronization boundaries. Body parse errors preserve
the coordinate and category when framing is intact.

Fallback: deterministic **all-record** index (including unsupported/errors), never
index among accepted events; bind framing/parser policy and verify index-to-byte
mapping. It requires a freeze amendment if offsets cannot be retained. Parser
iteration order alone is insufficient. Pin expansion order: conventional withdrawals
in wire order, conventional announcements, then MP attributes in wire order and
their prefixes in wire order, preserving path IDs. Counting-policy changes create
a distinct namespace and a reviewed crosswalk, not in-place IDs.

Redelivery of one coordinate/ordinal is suppressed; different coordinates with
identical payload/time survive. Equal fingerprints are not retry IDs. Preserve
all 118,172 duplicate-looking fingerprints from the original policy. The 57
excluded parser records remain an auditable exclusion ledger, not valid events.
Preserve second timestamps and any extended precision separately; primary Phase 13
rate policy used integer source epochs. Same-time ordering can use artifact/offset
as deterministic presentation order, never as proven network causality.

Overlapping archives retain artifact-specific occurrences unless source equivalence
is proven; exact duplicate artifact bytes at another URL are aliases. Revised
artifacts form alternative input revisions, not both one magically deduplicated
world event stream. Non-unicast/unsupported types remain categorized outside the
accepted projection; ADD-PATH, MP expansion and peer-context fixtures are required.
No RIB baseline or complete session/update evidence exists for full route-state claims.

## Shared method versus shared implementation

| Shared concern | Terraform | RPKI | DNS | RIS |
|---|---|---|---|---|
| Source identity | Commit/path/incarnation | Descriptor+row/tuple | Partition+row/group | Artifact+offset+expansion |
| Temporal honesty | Ordinals, committer time | Day/interval, unknown validation instant | Worker time/range and unknown cause | Source occurrence time, ties/multiplicity |
| Completeness | Accepted first-parent scope | Independent complete-set barrier | Partition and entity evidence distinguished | Framed parsed subset+errors, not global network |
| Deduplication | Snapshot retry | Row retry; set canonicalization separate | Row retry; value reuse retains observations | Coordinate retry, never fingerprint dedup |
| Recovery/publication | Existing checkpoint/batch manifest | Same convention plus day completion | Same plus partition/group qualifiers | Same plus artifact occurrence accounting |
| Reconstruction | Accepted source version | Sampled day set | Observed qualified projection | Occurrences, not route state |
| Provenance/evaluation | Frozen manifests, identities and query inputs for every domain | Same principles, set oracle | Same principles, measurement oracle | Same principles, expansion ledger |

Shared runtime, commit helpers, safe logging, environment/progress collection and
test perturbations are reusable. Configuration version state, text diff and router
rules are not reusable set/event logic. Domain parsers, canonicalizers, oracles
and analytics stay separate. One methodology is defensible without pretending
one processor or universal change record can handle every domain.
