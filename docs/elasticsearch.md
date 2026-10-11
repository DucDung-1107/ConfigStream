# Elasticsearch derived serving layer (Phase 8)

## Authority and scope

MinIO's committed Parquet history is authoritative. Elasticsearch is a disposable
projection for operational search, not the only copy of configuration history.
Phase 8 adds no API, UI, historical analytics job, ILM, or production cluster.

The existing single Structured Streaming query still consumes `config.snapshots`
directly. Its version-aware state, normalizer, diff IDs, rules and Alert contract
are unchanged. Search requires lake mode and defaults OFF.

The output boundary is deliberately ordered:

1. Commit raw/processed Parquet and the Phase 7 manifest.
2. Index safe processed projections into Elasticsearch from Spark partitions.
3. Publish existing Alert envelopes to Kafka.
4. Return so Spark can complete its checkpointed batch.

There is no distributed transaction or end-to-end exactly-once guarantee. A search
failure fails the query, leaving committed lake history available. Restart with
the same checkpoint retries that batch, preserves committed Parquet, and replaces
Elasticsearch documents with the same logical IDs. A failure after Kafka delivery
can still duplicate physical Alert messages. Protect the checkpoint and bucket.

## Pinned versions and integration choice

| Component | Version |
| --- | --- |
| Elasticsearch server | **9.5.5** |
| Docker image | `docker.elastic.co/elasticsearch/elasticsearch:9.5.5` |
| Official Python client | **elasticsearch 9.5.1** |
| Python / Spark / Scala / Java | 3.12 / 4.0.2 / 2.13 / 21, unchanged |

Selection was checked against official [server release notes](https://www.elastic.co/docs/release-notes/elasticsearch),
[client release notes](https://www.elastic.co/docs/release-notes/elasticsearch/clients/python),
and [support policy](https://www.elastic.co/support/eol). Client 9.5.1 supports
Python 3.12 and the same server major/minor; dependencies are locked in `uv.lock`.
The Docker manifest provides native linux/amd64 and linux/arm64 images.

This phase uses the official Python client in worker partitions rather than adding
elasticsearch-hadoop JARs. Published [Hadoop requirements](https://www.elastic.co/docs/reference/elasticsearch-hadoop/requirements)
did not establish compatibility for this pinned Spark 4.0.2 runtime. The worker
Bulk API boundary avoids a speculative Scala/Spark connector combination, while
keeping rows distributed. No DataFrame is collected wholesale on the driver.

## Serving contract v1

Physical indices are versioned; queries and writes use stable aliases:

| Physical index | Alias | Document `_id` |
| --- | --- | --- |
| `configstream-snapshots-v1` | `configstream-snapshots` | snapshot_id |
| `configstream-diffs-v1` | `configstream-diffs` | diff_id |
| `configstream-alerts-v1` | `configstream-alerts` | alert_id |
| `configstream-diagnostics-v1` | `configstream-diagnostics` | diagnostic_id |

All mappings use `dynamic: strict`, `_meta.search_schema_version=v1`, one primary
shard and zero replicas for local development only. `search-init` checks the server
version, creates missing indices/aliases, and verifies exact mappings, local
settings, and a single write-index alias target. It never deletes an incompatible
index. Future incompatible changes require a new physical schema version and an
explicit alias migration, not silently mutating v1.

Snapshot fields: snapshot_id, asset_id, version, event_time, ingest_time, raw_hash,
normalized_hash, normalization_version. Diff fields: schema_version, diff_id,
asset_id, old_snapshot_id, new_snapshot_id, added, removed, changed, event_time.
Alert fields are the existing schema: schema_version, alert_id, asset_id,
snapshot_id, rule_id, severity, message, created_at.

IDs/category fields are keyword; versions/counters/Kafka offsets are long;
timestamps are UTC `date_nanos` preserving microseconds. Hashes are stored keyword
fields with indexing/doc_values disabled. Alert message is text and comes from
the trusted rule definition, not arbitrary config-line interpolation.
The [date_nanos range](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/date_nanos)
is approximately 1970–2262; out-of-range dates fail indexing rather than silently
losing precision. Hourly aggregations have millisecond resolution.

Diagnostics use a controlled reason/type, asset/snapshot identity when known,
version(s), missing-version range, Kafka provenance when available, event time and
watermark timing. Invalid input has `diagnostic_type=invalid_record` and a separate
validation reason. State conditions retain types such as `version_conflict`,
`unresolved_version_gap`, `state_eviction` and `state_timeout`. Reasons are
allow-listed; arbitrary summary fields and Kafka keys are excluded.
When an invalid payload has no trusted event_time, the search diagnostic's time
falls back to its retained Kafka timestamp, enabling recent-diagnostic time filters.
This is a diagnostic-only fallback, not the snapshot watermark's event-time policy.

Diagnostic ID is `diag-` plus SHA-256 of a policy-version prefix and canonical
sorted JSON of the safe projected fields, with UTC-normalized timestamps. Query
namespace, wall-clock time and UUID4 are not used. The same stored diagnostic
rebuilds to the same ID. Distinct invalid offsets remain distinct diagnostics.

## Security policy

Never index raw Kafka bytes, source envelopes, raw content, content_uri bodies,
full normalized_content, or full diff details. Projection uses field allowlists
before transport; worker-side Spark serialization removes configuration columns.
`CONFIGSTREAM_SEARCH_DIFF_DETAILS=true` is rejected: v1 intentionally has no
diff-line search. Conservative masking is not proof that configuration is safe.

Metadata identifiers and trusted rule messages remain operational data; protect
the search service too. Logs contain counts/statuses/timing, not documents or
credentials. Root MinIO credentials remain private and ignored as in Phase 7.

Local Elasticsearch disables authentication/TLS and binds only to
`127.0.0.1:9200` on the host. It is **not production-ready**. Other containers on
the Compose network can reach it; do not attach untrusted containers or publish
this endpoint through tunnels. Username/password configuration is supported for
an external secured endpoint, with normal TLS verification; do not commit values.

## Bulk writer

One client is scoped to each Spark partition and closed after use. Metadata rows
become deterministic `_id` index actions, using `require_alias=true`. Each request
is bounded by the configured document count (default 1000) and a 5 MiB byte limit.
An oversized single document is rejected safely. No per-document HTTP requests.

Every Bulk API item is inspected. Successful items are counted; only transient
failed items are retried. 429/500/502/503/504 and connection/timeout failures use up
to three retries with 1/2/4-second backoff. Mapping/auth/missing-alias failures fail
immediately. Exhaustion and malformed responses fail the Spark batch. Transport
automatic retries are disabled to keep the retry bound explicit.

Partition logs report attempted/indexed documents, requests, retries, failures and
elapsed seconds. A task retry may repeat requests, but index-by-ID replaces logical
documents. No throughput, production latency or cluster-scaling claim is made.

## Setup and live demo

```sh
make search-setup
make lake-env
set -a; . .cache/minio.env; set +a
make search-up
make kafka-topics
make lake-init
make search-init
```

`search-up` starts Kafka, MinIO and Elasticsearch with health checks, preserving
existing named volumes. Kafka-only and lake-only commands still omit Elasticsearch.
The optional `search` extra is not installed by `make setup` or ordinary CI.

Terminal A, with credentials exported and a fresh lake-compatible checkpoint:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase8-demo \
CONFIGSTREAM_LAKE_PREFIX=phase8-demo make search-stream
```

After initial query progress, terminal B:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase8-router --start-time 2026-10-11T00:00:00Z'
make search-inspect
make search-query SEARCH_ARGS='snapshots --asset-id phase8-router --latest'
make search-query SEARCH_ARGS='diffs --asset-id phase8-router'
make search-query SEARCH_ARGS='alerts --severity high --start 2026-10-11T00:00:00Z --end 2026-10-12T00:00:00Z'
make search-query SEARCH_ARGS='alerts --group-by severity'
make search-query SEARCH_ARGS='alerts --group-by rule_id'
make search-query SEARCH_ARGS='alerts --group-by asset_id'
make search-query SEARCH_ARGS='diffs --group-by diffs_per_hour'
make search-query SEARCH_ARGS='diagnostics --type version_conflict'
```

In empty indices, this history yields 3 snapshots, 2 adjacent diffs and 2 alerts,
with latest version 3 and rules logging-disabled/telnet-enabled. Counts include all
earlier documents under the selected prefix; choose an unused
`CONFIGSTREAM_SEARCH_INDEX_PREFIX` for an isolated demo. `search-inspect` explicitly
refreshes indices; other queries respect Elasticsearch's near-real-time refresh.
Normal filters are exact term/time filters, with up to 1000 hits. Aggregations
return top 100 severity/rule/asset buckets or UTC hourly diff counts; they are
operational queries, not historical analytical jobs or exhaustive pagination.
Group-by CLI queries are unfiltered and reject combining document filters with
aggregation flags. Diff counts include normalized-empty version-pair diffs.

## Rebuild committed history

Stop the live writer before intentionally rebuilding empty indices. Remove only
the four known physical indices for your development prefix using Elasticsearch's
explicit index-delete API; there is deliberately no destructive wildcard/reset
command. Never delete lake data to rebuild search.

```sh
make search-init
CONFIGSTREAM_LAKE_PREFIX=phase8-demo make search-reindex
CONFIGSTREAM_LAKE_PREFIX=phase8-demo make search-reindex
make search-inspect
```

`search-reindex` uses Spark batch reads through Phase 7 `read_committed`, never a
recursive scan of partial objects. Optional `SEARCH_ARGS='--query-id <UUID>'`
limits one lake query. It applies the same safe projections and worker bulk writer
as live processing. Repeated indexing replaces documents by ID, so logical counts
remain stable. It does not delete old search-only documents; a complete rebuild
starts from explicitly recreated empty indices. Multiple retained query namespaces
can contain the same logical IDs; differing payloads for an identical ID require
source conflict resolution, not an arbitrary latest-arrival guarantee.

## Settings, tests and boundaries

Central settings: `CONFIGSTREAM_SEARCH_ENABLED=false`,
`ELASTICSEARCH_URL=http://localhost:9200`,
`CONFIGSTREAM_SEARCH_INDEX_PREFIX=configstream`,
`CONFIGSTREAM_SEARCH_BULK_SIZE=1000`,
`CONFIGSTREAM_SEARCH_TIMEOUT_SECONDS=30`,
`CONFIGSTREAM_SEARCH_DIFF_DETAILS=false`, and optional paired
`ELASTICSEARCH_USERNAME`/`ELASTICSEARCH_PASSWORD`. Keep live search lake-enabled.
Existing v7 lake query topology/state is unchanged; use a new checkpoint for a
fresh demo, or retain the same compatible checkpoint for recovery. Starting offsets
do not override checkpointed progress.

```sh
make check
make spark-test
make integration-test
make spark-integration-test
make lake-integration-test
make search-integration-test
make search-down
```

Unit tests cover projection security, mappings/aliases/config, deterministic IDs,
bounded bulk writes, partial failures and retries. Service-free Spark tests check
UTC serialization, safe column exclusion and multi-sink order/failure behavior.
Explicit real-service tests cover strict mappings, upserts, adjacent out-of-order
pairs, duplicates, checkpoint restart after worker-side connection failure,
unchanged committed Parquet, keyed Kafka alerts, queries/aggregations, and deleting
only test-owned indices before two lake rebuilds. Ordinary CI does not run Docker.

No HA, lifecycle/rollover, compaction, atomic multi-service transaction, cross-query
source-conflict resolution, or final performance benchmark. Phase 9 should keep
FastAPI thin: query these aliases/helpers, bound requests, and design authorized
configuration retrieval separately from this metadata-only search projection.

## Recorded local acceptance: 2026-10-11

The macOS arm64 local stack ran the pinned server/client, existing Kafka/MinIO,
Spark 4.0.2 and Java 21. This is correctness acceptance, not a performance benchmark.
The explicit test commands cover 637 unit tests, 50 service-free Spark tests,
3 Kafka tests, 3 Spark/Kafka tests, 1 lake integration and 2 search integrations.

The real search pipeline test verified two independent assets (normal and
out-of-order), duplicate snapshots, a version conflict and four invalid records.
It stopped/restarted separate Spark processes with the same checkpoint and
injected a real worker connection refusal after lake commit. Recovery retained
Parquet ETags and produced **6 snapshot docs, 4 diffs, 4 alerts, 5 diagnostics**.
Kafka retained four schema-valid keyed alerts. Recreating the four test-owned
indices and rebuilding twice retained exactly the same counts and logical IDs.
Queries found both canonical adjacent pairs per asset, high/critical alerts,
one conflict, four invalid records, severity/rule/asset buckets and hourly diffs.

A separate continuous CLI demo used:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase8-clean-live-20261011 \
CONFIGSTREAM_LAKE_PREFIX=phase8-clean-live-20261011 \
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase8-live-20261011 make search-stream
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase8-router --start-time 2026-10-11T00:00:00Z'
```

With the search prefix exported in the query terminal, `make search-inspect`
returned **3 snapshots, 2 diffs, 2 alerts, 0 diagnostics**. Latest metadata was v3;
the diffs were v1→v2 and v2→v3. Separate high/critical time-range queries returned
logging-disabled and telnet-enabled respectively. Severity and rule aggregation
each returned two one-document buckets; asset aggregation returned two alerts
for phase8-router, and the UTC hourly bucket contained two diffs.

After a clean SIGINT shutdown, `make lake-inspect` with
`CONFIGSTREAM_LAKE_PREFIX=phase8-clean-live-20261011` confirmed **3 raw snapshots,
3 normalized snapshots, 2 diffs, 2 alerts, 0 diagnostics**. Only the four
`phase8-live-20261011-*-v1` indices were explicitly removed (no wildcard was passed).
`make search-init` recreated empty indices. Two `make search-reindex` CLI runs,
with the same lake/search prefixes and `SPARK_MASTER=local[2]`, each restored and
retained the original **3/2/2/0** document counts. MinIO data was not deleted.

Validation commands were `make check`, `make spark-test`, `make integration-test`,
`make spark-integration-test`, `make lake-integration-test`, and
`make search-integration-test`. Query commands are listed above; setup used
`make search-setup`, `make search-up`, `make kafka-topics`, `make lake-init`,
and `make search-init`, with private credentials exported from `.cache/minio.env`.
