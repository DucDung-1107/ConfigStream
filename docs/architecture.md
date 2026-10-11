# Architecture

## Scope and status

ConfigStream focuses on distributed configuration-change storage and processing.
Phases 0–9 supply a Python project, tested versioned schemas, a stateful generator,
JSONL output, Kafka ingestion with an asset registry, and direct Spark Structured
Streaming parsing/validation, native version buffers, normalization/diff, and YAML
rules publishing Kafka alerts, plus optional MinIO/Parquet history and a rebuildable
Elasticsearch metadata projection.
The generator and pure processing package can run without external services.
A thin read-only FastAPI boundary now serves the Elasticsearch projection.
Kubernetes and monitoring systems remain future work.

## Phase 10 planes

- Streaming: curated real-baseline-derived histories or the original synthetic
  generator → Kafka → native stateful Structured Streaming → committed lake/search.
- Batch: a frozen set of committed Parquet manifests → Spark DataFrame aggregations
  and prior-window thresholds → typed `analytics/` Parquet plus completion manifests.
- Serving: Elasticsearch's metadata-only projection → read-only FastAPI. This does
  not replace historical Spark analytics and is not the source of truth.

The replay harness measures local fleet/version/retention workloads with fresh
checkpoints, prefixes and explicit Kafka offsets. High rates are accelerated history
replay/stress, not natural device-frequency claims. External corpus retrieval and
large experiments never run in ordinary CI. See [datasets](datasets.md),
[historical analytics](historical-analytics.md), and [benchmark methodology](benchmark.md).

## Implemented synthetic source

`generator/engine.py` maintains a list of per-asset states and selects an asset
using a local seeded RNG. Type-specific templates and mutation domains are data;
the mutation and versioning logic is shared across all asset types. Each mutation
constructs the existing ChangeEvent and ConfigSnapshot models, then advances only
the selected state. CLI pacing is shared across JSONL and Kafka sinks, outside
the engine. `messaging/serialization.py` is the common envelope encoder;
`generator/serialization.py` retains its Phase 2 compatibility interface.

`messaging/kafka_producer.py` accepts domain models and owns routing, asset keys,
callback polling, bounded backpressure, delivery failures, and final flushing.
confluent-kafka is an optional, lazily imported dependency; generator state and
schemas have no dependency on its internals. See [generator lifecycle](generator.md).

## Implemented Kafka ingestion

Kafka is the ingestion/message backbone, not an application database. At startup,
Kafka mode sends N Asset envelopes to `config.assets`, keyed by `asset_id`, and
waits for this registry batch to be acknowledged before generating changes. It
does not resend Asset on every mutation. A later update may publish the same key;
the compacted registry retains its latest value eventually, not immediately.

For each change, the producer enqueues ChangeEvent to `config.events` and
ConfigSnapshot to `config.snapshots`. Both use the validated asset ID as UTF-8 key.
Ordering applies **only within a partition of a topic**. There is no cross-topic
atomicity or ordering, including the asset registry; separate consumers may
observe records in a different order. Never depend on event-before-snapshot or
snapshot-after-event arrival. Future enrichment must handle a missing asset row.

`config.snapshots` is the primary Spark configuration-processing stream.
`config.events` is for audit, event analytics, triggers, and operational history;
it is not a necessary ordered precursor to processing a snapshot.

The producer enables Kafka idempotence and `acks=all`, but generator state is
in-memory and advances before delivery confirmation. Failures produce a nonzero
exit; successful process completion requires all queued deliveries to be confirmed.
Replay is deterministic but a new producer/run can append duplicates. No durable
generator checkpoint, transactions, or exactly-once end-to-end guarantee exists.

Compose runs one combined KRaft controller/broker with replication factor 1,
automatic topic creation disabled, loopback-only plaintext exposure, and persistent
Docker volume storage. `make kafka-topics` owns topic initialization/verification.
This is a local development topology without HA or authentication. See
[topic configuration, retention, and local commands](kafka-topics.md).

## Implemented Spark consumer

Host-local Spark 4.0.2 uses `readStream.format("kafka")` to subscribe exclusively
to `config.snapshots`. No Python Kafka consumer feeds the processing path. SQL
`from_json` and a separate StructType parse the existing JSON envelope. SQL
expressions validate basic fields, UTC timestamps, content-location exclusivity,
and Kafka key equality; no per-row Python/Pydantic UDF is involved.

The classified streaming DataFrame retains snapshot content and Kafka provenance.
A valid-snapshot projection exposes the clean transport fields and metadata.
Phase 6 sends valid snapshots through watermark-aware ID deduplication and
`groupBy(asset_id).transformWithStateInPandas`. Invalid records bypass business
state and retain the Phase 4 safe diagnostic projection. Their output is unioned
with stateful diffs/alerts into one checkpointed query and one `foreachBatch`.
The batch is cached for counts, bounded safe logging, and distributed Kafka alert
writes, then unpersisted. Lake mode also archives original Kafka bytes before
dedup/late filtering and accepted normalized snapshots into partitioned Parquet.
A per-batch manifest publishes complete lake output before optional Elasticsearch
indexing and the Kafka alert write. Elasticsearch failure fails that batch; retry
preserves committed Parquet and replaces search documents by deterministic ID.
Malformed records remain visible without config logging.
The optional `--parse-only` console query remains available with its own checkpoint.

Checkpointed query progress supports local restart. Starting offsets apply only
without prior progress; Kafka retention/deletion can still prevent recovery. A
console display is not durable; Kafka alert retries can duplicate physical records.
Spark's native progress metrics are logged without claiming measured throughput. See
[version matrix, launch commands, and validation scope](spark-streaming.md).

## Component responsibilities

| Component | Responsibility and rationale |
| --- | --- |
| Sources and collector | Obtain configuration; generate IDs, versions, timestamps; publish records without processing business rules |
| Kafka | Decouple ingestion from compute; retain an ordered, replayable log per partition |
| Spark Structured Streaming | Parsing, watermark/dedup, native per-asset version buffers, normalization/diff, rules and Kafka alerts; live business-window aggregation remains future work |
| Pure processing | Text normalization, explicit same-asset pair comparison, bounded transitions and YAML rules; Spark owns distributed state persistence |
| MinIO + Parquet | S3-compatible local historical source of truth; columnar data for analytical reads |
| Elasticsearch | Derived, rebuildable serving/search indexes; not the authoritative historical store |
| Spark batch | Read historical Parquet for frequency, severity, and anomaly analytics |
| FastAPI | Thin query layer over Elasticsearch; no duplicate diff or rule logic |

## Implemented configuration processing

`processing` is independent of Spark and Kafka. A validated inline text
ConfigSnapshot becomes an immutable NormalizedConfig, with ordered canonical
text, conservative secret masking, and a separate SHA-256. Raw content/hashes
remain untouched. Explicit previous/current normalized snapshots produce the
existing ConfigDiff plus internal gap/hash diagnostics; no schema was extended.

The current boundaries are:

- Kafka → Spark parse/validate → native version state → normalization/diff/rules → Kafka alerts.
- ConfigSnapshot pair → pure normalization → deterministic diff (offline API).

Normalization/diff logic is now **reused by the stateful stream**.
The caller must select snapshots for the same asset with increasing versions;
cross-asset, duplicate, reverse, or reused snapshot identities fail clearly.
The pure offline API permits explicit gaps, but canonical streaming holds missing
versions and emits only adjacent pairs. Arrival order does not establish a prior
version. Spark owns durable state, timers, ID dedup, and edge flags; pure Python
transitions only decide changes to a bounded buffer. See [processing contracts](config-processing.md)
and [stateful policies and failure boundaries](stateful-processing.md).

## Streaming path

Collectors publish `ConfigSnapshot` and `ChangeEvent` records separately.
The initialized Kafka topics are `config.assets`, `config.snapshots`, `config.events`,
`config.alerts`, and `config.dlq`; only the DLQ has no producer. Use
**asset_id as the Kafka key** whenever per-asset ordering
matters. A single development broker is sufficient. Kafka ordering is within a
partition, not global and not across topics. Producers must coordinate version
assignment for the same asset; repartitioning and retries need explicit handling.

Spark now deduplicates IDs, normalizes content, compares adjacent versions per
asset, evaluates YAML rules, and publishes alerts. Optional lake mode persists
raw/processed history. Phase 10 calculates historical window metrics in the separate
batch plane. Future streaming stages may route malformed/unsupported records to
the DLQ and calculate live business-window metrics. The DLQ must preserve the
original payload, ingestion timestamp, and reason under controlled access.
Spark owns this state and computation; the collector and API do not emulate it.

`event_time` means when a source observed the change. `ingest_time` means when the
collector accepted it; neither is Spark processing time. Both must carry a
timezone. Event-time watermarks bound dedup and advance per-asset timers. A size cap
also bounds retained versions; cleanup needs event-time progress, not wall time.
Arrival order alone is not enough to identify the correct preceding version.

Checkpointed version state and deterministic diff/alert IDs are implemented;
future serving storage should upsert these IDs. Multiple sinks are not automatically
one atomic transaction; replay and partial-write recovery
must be designed and tested rather than advertised as end-to-end exactly-once.

## Storage and batch path

Implemented logical locations under the private `config-lake` bucket:

- `raw/`: all fetched original snapshot-topic records and Kafka provenance, access restricted.
- `processed/`: accepted normalized snapshots, diffs, alerts and safe diagnostics in Parquet.
- `analytics/`: typed Phase 10 historical aggregates, keyed by deterministic input/parameter run IDs.

Processed snapshots are partitioned by UTC event date (`year`, `month`, `day`),
inside a query/batch namespace for replay-safe writes. There is no asset_id
partition. Asset type is not in ConfigSnapshot, so type partitions are deferred
until an explicit registry join/enrichment. File sizes and compaction need later
measurement. The committed-reader helper excludes incomplete batches; MinIO and
Kafka writes are not one transaction. See [lake schemas, recovery and commands](data-lake.md).

Elasticsearch now holds strict, versioned indices/aliases for snapshot metadata,
diff summaries, Alerts and safe diagnostics. Raw/full normalized configuration
and diff lines stay out of search. Worker partitions use bounded Bulk API writes;
batch rebuilds read only committed Parquet through the existing reader contract.
Elasticsearch is derived and disposable, while MinIO remains authoritative.
See [search mappings, security, rebuild and failure semantics](elasticsearch.md).
Asset/event/metric projections are not implemented here. Future batch jobs read
the lake independently of the serving layer,
calculating changes by asset/hour/day/type, severity counts, frequently changing
assets, and later rolling mean/standard-deviation/z-score anomalies.

## Read-only serving API

`api.app:create_app` owns one Elasticsearch client per lifespan. Synchronous
backend-facing handlers run in FastAPI's threadpool and call `search.queries`,
not Spark, Kafka or the lake. The service validates safe `_source` projections
through response models and maps backend failures to sanitized HTTP errors.
Business endpoints are under `/api/v1`; cursor pagination uses time plus stable
domain ID tie-breakers and bounded `search_after` requests. Lightweight serving
aggregations are not historical Spark analytics.

Liveness is independent of the backend. Readiness requires Elasticsearch and the
four search aliases, not ingestion services. There is no authoritative API cache;
rebuilding Elasticsearch from committed history restores HTTP-visible records
without changing API state. Default exposure is unauthenticated loopback-only,
with no CORS or config-body retrieval. See [API contracts and acceptance](api.md).

## Boundaries and security

- Asset metadata is extensible; a network device is one asset type, not the schema.
- Snapshot content can be inline initially and referenced by `content_uri` later.
- Phase 1 verifies inline raw-content hashes and local record consistency. It
  neither reads URI content nor checks cross-record references, deduplicates IDs,
  or enforces sequential versions. Those operations need producer/processor state.
- Raw configuration may contain credentials. It is never safe merely because it
  passes schema validation. Mask before serving; restrict raw lake/DLQ access.
- Schemas must not log payloads on validation failure without sanitization.
- Configuration comes from environment variables. `.env.example` contains no secrets.
- Docker Compose is the first deployment target; Kubernetes and cloud are deferred.

## Incremental acceptance gates

1. Bootstrap installs and tests on Python 3.12.
2. All five versioned domain contracts validate and JSON round-trip.
3. Stateful generator produces meaningful consecutive versions on the console.
4. Kafka consumer CLI observes keyed messages; Spark then parses them to console.
5. Pure normalization and explicit-pair diff are tested without services.
6. Rules, event-time version state, out-of-order pairs, and process restart are tested in Spark.
7. Lake and serving sinks preserve history and expose results through FastAPI.

The MVP scenario changes `logging enabled` to `logging disabled`, preserves both
versions in the lake, and exposes a diff plus a high-severity `logging-disabled`
alert. Phase 9 tests this through the real Kafka/Spark/lake/search path and the
HTTP serving boundary, including out-of-order v1, v3, v2, same-checkpoint restart
and two lake-to-search rebuilds with the API alive.
