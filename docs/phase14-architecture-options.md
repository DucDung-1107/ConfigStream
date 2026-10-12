# Phase 14 — architecture and semantic integrity

Paper design only, against baseline `8026ae1`. No contracts, topics, pipelines,
lake zones or endpoints are implemented here. Decision and selected scope:
[GO — REDUCED SCOPE](phase14-go-no-go.md).

## What the existing code actually guarantees

| Inspected implementation | Genuine reuse | Boundary / required future change |
|---|---|---|
| [Asset](../schemas/asset.py), [ConfigSnapshot](../schemas/snapshot.py) | Extensible asset type/metadata; strict UTC times, positive ordinal, UTF-8 SHA-256 | Snapshot requires exactly one text/URI content location and a per-asset version; no observation completeness or typed set/event payload |
| [ChangeEvent](../schemas/event.py), [ConfigDiff](../schemas/diff.py), [Alert](../schemas/alert.py) | Source/time metadata and safe validation conventions | ChangeEvent can label other event types but lacks a typed routing payload/occurrence contract; arbitrary metadata is not sufficient. ConfigDiff counts textual added/removed lines, with changed bounded by paired lines. Alert refers to a configuration snapshot/rule, not a routing incident |
| [Messaging serializer](../messaging/serialization.py), [producer](../messaging/kafka_producer.py) | Idempotent Kafka-client settings, bounded enqueue/flush, callbacks, error patterns | Record union/type/topic dispatch are closed to existing models; producer keys `asset_id`. New typed serializers/keys require explicit registration, not duck typing |
| [SQL validation](../spark/streaming/validation.py), [native state](../spark/streaming/stateful.py), [pure state](../processing/state.py) | Direct Spark Kafka source, checkpoint management, safe diagnostic pattern, version-aware configuration regression | Validator only accepts `config_snapshot`; dedup is `snapshot_id` within watermark; state groups by asset and pairs integer N/N+1. None is a general set or BGP engine |
| [SparkConfig](../spark/common/config.py) | Runtime/dependency/environment plumbing | Defaults: 10-minute watermark, 30-minute timeout, 32 retained versions; not suitable unmodified for historical sparse states. No silent default migration |
| [Lake writer/reader](../spark/streaming/sinks/lake.py), [manifest client](../lake/client.py), [schemas](../spark/common/lake_schemas.py) | Query UUID/batch namespace, conditional manifest publication, S3A writes, committed-only reads | Dataset/schema allowlists and projections are configuration-specific. Reader validates exact dataset paths. Extend a narrow typed registry or analogous shared helper; do not call it a ready generic lake |
| [Analytics runner](../analytics/main.py), [jobs](../analytics/jobs.py) | Frozen input manifests, deterministic parameter/run identity, result completion marker | Loader knows snapshots/diffs/alerts/diagnostics; jobs use configuration columns. State reconstruction and event counts need separate typed jobs. Current calendar expansion is inappropriate for every prefix across years |
| [Benchmark metrics](../benchmarks/metrics.py), [harness](../benchmarks/main.py) | Environment/RSS/progress/lag capture and reporting pattern | Harness assumes config topic, publisher and adjacent pairs. Reuse metrics, not its snapshot throughput interpretation or whole execution method |
| [Search projections](../search/documents.py), [API responses](../api/models.py) | Metadata-only security boundary and rebuild concept | Strict snapshot/diff/alert mappings are not new-domain APIs. Keep unchanged; no new serving work in MVP |

The schema's `schema_version` describes wire format, not infrastructure version.
ConfigSnapshot's event time and ingest time do not supply an independent
measurement time or date-completeness marker. Asset alone is broadly usable;
reusing every other class through invented ordinals/text strings would retain
syntactic validity while losing meaning.

Inspected regressions include
[version permutations/conflicts/eviction](../tests/unit/test_state_logic.py),
[native state](../tests/spark/test_stateful.py),
[lake projection/publication](../tests/spark/test_lake.py), and
[analytics](../tests/spark/test_analytics.py). They protect the existing configuration
contract, not proposed RPKI/RIS behavior. No Phase 14 end-to-end validation occurred.

## Table B — architectural alternatives

Effort is relative; the selected design's total hours are separately budgeted.

| Design | Semantic correctness | Actual reuse | Effort / migration | Test complexity | Scale and maintainability |
|---|---|---|---|---|---|
| 1: narrow adapters into current snapshot pipeline | Good for Git files; poor if sets/events become fake files/ordinals | Highest apparent reuse of parsing/state/rules | Low initial effort, high semantic repair risk; can silently invalidate historical meaning | Existing tests still pass while new semantics can be wrong | Large serialized sets create hot keys; event version buffers unnecessary; reject for RIS and typed policy transitions |
| 2: shared provenance/time envelope, typed processing **recommended** | Good if shared fields stop at observation/occurrence identity and domain payloads stay typed | Source manifest, transport/backpressure patterns, recovery/publication helpers, metrics and evaluation harness | Moderate; preserve config v1, introduce additive new contracts and isolated checkpoints/dataset registry | Common failure/identity suite plus independent set/event oracles | Allows DataFrame joins for complete daily sets and append-only event processing; no universal state machine; small registry is maintainable |
| 3: separate pipelines, shared lake | Good domain meaning | Mainly storage/provenance/reporting; same services are not code reuse | Moderate-high; duplicated transport/recovery code unless intentionally factored | Several service suites and drift across recovery policies | Clear isolation but four projects if publication/metrics/tests are duplicated; acceptable physical execution under design 2, not a separate research claim |

Choose **design 2 logically**, with separate checkpointed queries where semantics
require them. It is not one giant unioned DataFrame with a universal schema. Design 3
is a legitimate deployment organization, but unifying only storage paths is too weak.
Do not extract a general framework before two concrete typed implementations exist.
No concurrent writer, global transaction manager or new database is justified.

## Domain semantics: explicit comparison

| Property | Terraform (reference) | RPKI (selected) | DNS (reviewed, postponed) | RIS (selected) |
|---|---|---|---|---|
| Entity / stable scope | Repository + path incarnation; deletion/recreation splits identity; rename split | Trust-anchor/validator coverage + ASN + canonical prefix + maxLength tuple; set scoped to source | Measurement program/scope + canonical query/response name and type; compare like filters | Collector + peer ASN/IP/session context + AFI/SAFI + prefix; not an asset-file version |
| Source time | Git committer timestamp | Published snapshot date; Not Before/After are validity fields, **not edit times** | Measurement timestamp (epoch milliseconds in tested schema) | MRT source epoch seconds in tested records; preserve finer precision when available |
| Observation time | Git content observation tied to selected commit; not deployment time | Validator collection time if supplied, otherwise unknown; date is not an invented precise command time | Probe/measurement timestamp; distinguish daily partition date | Collector observation time is the source time; archive publication time separate if known |
| Ingestion time | Actual importer/publisher receipt | Actual importer receipt | Actual importer receipt | Actual importer receipt; no reconstructed arrival-time claim |
| Baseline | First accepted file content, not a diff | First complete comparable authorization set; absence before this is unknown | First observation, not previous authoritative state; missing/error responses are not empty RRsets | Update-only window has **no full RIB baseline**; prefix event ledger can still be complete relative to parsed input |
| Update/change | Changed bytes, assigned ordinal; adjacent normalized text diff | Presence additions/removals between complete comparable daily sets | Address-set observation difference under fixed filters; uncertain cause | Announcement/withdrawal occurrence; retain attributes/path ID; no ConfigDiff |
| Deletion/removal | Ends incarnation; no deletion snapshot | Tuple no longer in next comparable validated set, not confirmed operator deletion | No response/missing group != proven zone deletion | Withdrawal observed from a peer, not device/config removal |
| Duplicate | Same immutable source snapshot identity; changed ID/content conflict is error | Same row delivery/coordinate or tuple duplicate inside one observation; repeated next-day presence is stability evidence | Same observation/row delivery; later identical observed set remains a new observation | Same source coordinate delivered twice is retry; identical-looking **different** occurrences are preserved |
| Ordering | Git extraction order/ordinal; timestamps can tie or reverse | Publication date plus comparable source/validation regime; complete-day barrier | Daily observation identity, actual sample times and completeness/filter compatibility | Preserve source archive record/expansion order; timestamp ties need coordinate tie-break; do not invent global routing causality |
| Historical queries | File content at observed ordinal, adjacent comparisons | Observed set at a complete date, added/removed by ASN/window | Measured address set at observation time, uncertainty/missingness | Event counts/bursts/peer-prefix occurrences; **not complete route state** |
| Retention/privacy | Immutable experiment inputs; secrets whole-history filtered, no raw Git publication | Scoped replay inputs/derived history for experiment; public topology can still be sensitive in combinations | Restricted noncommercial/share-alike inputs; avoid TXT/SOA contact fields, report aggregates | Immutable selected occurrences; peer IP/prefix topology metadata, no unnecessary public raw dump |

The RPKI canonical tuple excludes URI and certificate validity from set identity,
but raw observations retain them for provenance. A tuple policy relation is not
one persistent ROA object: several ROAs can yield equivalent tuples. The tested
RIPE trust anchor is not all trust anchors. DNS comparison uses only successful
A/AAAA groups, not the entire DNS state. See the original
[RPKI](phase13-feasibility-methodology.md) and
[DNS counting policy](../workloads/results/dataset-feasibility/dns.json).

## Essential paper contracts (no production classes)

Shared **SourceProvenance** is limited to: source/dataset ID, immutable artifact URL
and SHA-256, scope, selection manifest ID, parser/canonicalization version, original
timestamp/precision, observation interval/date where supplied, and ingest time.
Unknown times remain null with reason. A `record_kind` selects a typed payload.
Never assign Git versions or claim deployment time to fill missing fields.

- **VersionedConfiguration:** unchanged existing ConfigSnapshot plus Git sidecar.
  Version ordering stays source extraction ordinal; metadata is not generalized
  into a new universal payload.
- **StateObservation:** observation ID, source scope, entity/tuple key, canonical
  state/hash and observation date/time. Include quality/validation/filter regime.
  A separate **observation-completion descriptor** references input artifact/hash,
  accepted row count, canonical set checksum and completeness status. Row arrival
  alone cannot establish absence or removal.
- **StateTransition:** before/after observation IDs, entity/tuple, typed add/remove,
  comparison policy version and interval/gap status. DNS, if ever added, needs
  measurement quality/value-set semantics; it must not inherit policy certainty.
- **NetworkEvent:** source-occurrence ID, collector/peer, source timestamp, action,
  AFI/SAFI, prefix, optional path ID, typed path/attributes, source record coordinate
  and expansion ordinal. No invented per-prefix positive version.

These are three payload categories, not a generic bag of optional columns.
Essential shared identities are **source occurrences** and **published observation
boundaries**; not every domain has state versions. Retain immutable raw source
artifacts under access-controlled caches/storage. Derived data references them;
do not duplicate whole MRT archives inside every expanded prefix record.

RIS occurrence ID proposal: hash(source scope, artifact SHA-256, decompressed MRT
record byte offset, NLRI expansion ordinal, counting-policy version). This preserves
same-second, same-payload events at distinct coordinates and suppresses only
re-delivery of the same source occurrence. Include errors/unsupported records in
an audit ledger so offsets never depend on silently skipping records. A fingerprint
of time/peer/prefix/attributes is a **diagnostic**, not an ID. Revised artifacts at
the same URL require explicit reconciliation; do not call both independent events.
Distinct overlapping archive coordinates also require a declared selection policy,
not speculative payload deduplication.

RPKI observation ID proposal: hash(scope, declared date, artifact SHA-256,
canonicalization/validation regime). Canonical set members use tuple keys, not row
order. Same-date disagreeing artifacts are alternative source revisions to inspect,
not automatically successive policy versions. Derived transitions reference two
complete observations and a comparison contract. No absence inference until the
completion descriptor is committed and validated.

## Temporal correctness and ground truth

| Situation | Required policy / acceptance oracle |
|---|---|
| Late/out-of-order Terraform | Keep existing adjacency-by-version policy; historical override only; ordinal oracle from extractor |
| Out-of-order RPKI days / rows | Publish complete observations first, compare by date in batch; late day may complete a gap in a **new derived run**, never mutate published outputs silently |
| Missing/partial RPKI date | Record gap; no daily transition across it. Optional separately labeled interval-endpoint comparison is not the missing daily history |
| DNS failures / changed coverage | Missing/error/unsupported observation is unknown, not empty; compare only qualified common groups and equivalent filters |
| Same timestamp | Preserve Terraform ordinals and RIS record coordinates; dates/times do not uniquely identify events |
| Retry versus intentional repeat | Suppress duplicate source coordinate, retain all distinct coordinates; compare multisets rather than sets for RIS |
| Historical replay | Preserve source times; measure physical replay separately. Frozen input and query-specific offsets/checkpoints; never change demo defaults |
| Watermarks | Archive/preserve all fetched raw records before lateness policies. RPKI batch completeness is not a watermark; RIS archive ledger needs no state-version timeout. Future live windows may flag late input but cannot silently delete the audit history |
| Restart / partial publication | One writer per checkpoint; deterministic batch paths, manifest-last publication, same-checkpoint replay; verify missing/extra IDs after injected before/after-commit failures |
| New checkpoint / independent run | New query namespace can contain physical duplicates. A frozen-input logical reader must resolve identical occurrence IDs; batch count checks alone do not prove payload equality |
| Source revision / parser change | New dataset/derivation version and checkpoint; no automatic checkpoint migration or mixing incompatible counting rules |

RPKI oracle: independent canonical daily sets and adjacent differences, including
per-date digests, not just 12,016 total transitions. RIS oracle: per-artifact
source-coordinate multisets and per-minute action counts, not just 4,725,156
total events. Phase 13 aggregates alone cannot prove exact ID preservation:
Round 2 must derive a bounded coordinate/digest ledger from the **already cached**
inputs and verify it with a second implementation or hand-checked owned fixtures.
If parser corrections recover some of the 57 excluded records, publish a revised
counting-policy result alongside the original, never silently overwrite Phase 13.

Current `.dropDuplicatesWithinWatermark(["snapshot_id"])` is neither permanent
delivery deduplication nor appropriate for RIS payload fingerprints. A persisted
source-occurrence/manifest policy and logical cross-run read rule are required;
Kafka idempotent producer settings do not eliminate app-level replay. No atomic
transaction across Kafka/MinIO/search exists today or is promised by this review.

## Storage and query design, not new zones

Conceptually distinguish raw source evidence, canonical complete observations,
derived transitions, native occurrences and historical aggregates. Reuse current
MinIO/Parquet and single-writer commit protocol; add typed dataset registrations
only during a later approved implementation. No new transaction/table format now.

Partition by domain/source scope and UTC date, not ASN/prefix/entity high-cardinality
keys. Keep collector as a small scope dimension; if hourly partitions create
small files, use date plus event-time columns first. Sort/cluster within files only
as a separately measured layout variable. Entity-specific filters do not imply
an index: the current reader has no manifest/date pruning or catalog. A small
input-manifest inventory is sufficient for the MVP; full entity indexing is not.

Batch publication includes schema/contract version, source-manifest digest,
dataset paths/counts and completeness descriptor. Existing deterministic query/batch
directories and conditional commit remain authoritative. Orphan uncommitted objects
are invisible to readers; failed final publication is retried in its own namespace.
Do not claim multi-table serializability, concurrent writer locking or global ACID.

RPKI: retain original compressed CSV once; canonical daily sets and optional
baseline+transitions are alternative analytical layouts for the experiment.
Reconstruct **only complete observed dates** and explicitly delimit gaps. Retain
at least a baseline before any transition retention cut; otherwise reconstruction
is impossible. RIS: retain event occurrences and compact aggregates, not speculative
latest-route state. Event-time queries select a frozen input set. Long-term lossless
retention/compaction lifecycle is a policy question, not a production implementation.

Small files and manifest enumeration can dominate local queries. Use a declared
target file size/row cap and report file distribution; append-only compaction can
publish a new analytical run only if needed, never overwrite raw history. Compare
compressed source, normalized Parquet, transition Parquet, metadata and query
outputs separately. No universal production compression ratio.

## What precisely is unified?

Substantial shared responsibilities: immutable source selection and coordinate
provenance; replay/retry accounting; schema validation and safe error boundaries;
Kafka backpressure/offset handling; committed publication/recovery; frozen-input
historical reads; benchmark units/resource capture; the same adversarial test matrix.
Domain parsers, canonical keys, completeness meaning and analytical queries remain
different. Sharing those incorrectly is worse than writing small typed branches.

The recommended architecture is academically explainable as **one preservation
and cost experiment over two temporal information types**. It is not a universal
infrastructure model, a policy/routing correlation engine, or four fully independent
analytics products. If common publication/identity tests cannot be factored without
domain-specific exceptions in most fields, stop at the first new RPKI source and
reassess scope before building RIS service integration.
