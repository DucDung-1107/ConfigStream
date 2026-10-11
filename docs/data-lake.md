# MinIO and Parquet history (Phase 7)

## Implemented data flow

One Kafka Structured Streaming source still consumes only `config.snapshots`.
Lake mode extends the Phase 6 query with a branch for original Kafka records,
before parsing, watermarking, or deduplication. The existing native state processor
also emits each newly accepted normalized snapshot once. Both branches share the
same output schema and one checkpointed `foreachBatch` boundary.

The batch is persisted and fully consumed, then the lake sink writes Parquet and
publishes a small commit manifest. Optional Phase 8 Elasticsearch indexing runs
next, then Kafka alerts are written. Operational
logs continue to contain counts and safe metadata only. Spark writes rows on its
workers; the Python S3 client handles bucket initialization and manifests only.

Existing `make spark-stream`, generator JSONL, Kafka producer, and parse-only mode
remain usable with lake mode disabled. `make lake-stream` enables the lake.

## Runtime and local storage

Python 3.12, Spark/PySpark 4.0.2, Java 21, Scala 2.13, and Kafka 4.1.2 are unchanged.
The installed Spark distribution bundles Hadoop **3.4.1**. Lake mode checks that
version and resolves these pinned artifacts through Spark's existing Maven mechanism:

- `org.apache.hadoop:hadoop-aws:3.4.1`
- `org.apache.spark:spark-hadoop-cloud_2.13:4.0.2`
- Transitive AWS SDK Java bundle **2.25.53**.

The Python `lake` extra pins **boto3 1.40.45**, with all transitive versions locked
in `uv.lock`. Pure generator, processing, and configuration imports do not load it.
JARs, Python tools, temporary files, and credentials stay in ignored project-local
directories. The first Maven download includes a large SDK bundle; first startup
can take several minutes. Avoid concurrent first-start package downloads sharing
the same Ivy cache.

Compose's optional `lake` profile adds one MinIO server and a persistent named
volume. Ports 9000 (S3) and 9001 (console) bind to host loopback. The source build
pins MinIO release **RELEASE.2025-10-15T17-29-55Z**, commit
`9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, Go **1.24.9**, and Alpine **3.22.2**.
It builds natively for the Docker host architecture; arm64 was exercised locally.
The Dockerfile does not force amd64 emulation or install Go on the host.

The [community repository](https://github.com/minio/minio) is archived and describes
source-only distribution. Consequently this project builds the pinned
[security release](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z)
instead of depending on unavailable legacy images. This is a reproducible local
university demo, not a promise of maintained production storage.

## Historical datasets

The default bucket is private `config-lake`; creation never enables public access.

| Dataset | Stored fields and semantics |
| --- | --- |
| `raw/snapshots` | Exact binary Kafka key/value plus topic, partition, offset, timestamp; includes malformed, conflicting, duplicate, and late records |
| `processed/snapshots` | Accepted snapshot ID, asset ID, version, event/ingest time, raw hash, normalized hash/content, `normalization_version=text-v1` |
| `processed/diffs` | Existing ConfigDiff fields with structured line details and triggering event time |
| `processed/alerts` | Existing Alert fields, deterministic ID and event-time created_at |
| `processed/diagnostics` | Safe Phase 4/state diagnostics, without config bodies |
| `analytics/` | Reserved logical zone; no analytics job or fabricated dataset |

Raw history preserves both versions even when masking intentionally suppresses a
secret-value diff. It also preserves records dropped by business lateness/dedup
policies. "Raw" means the records fetched from retained Kafka offsets by this
query; data already deleted by Kafka retention cannot be reconstructed here.
The raw ledger is not a DLQ producer and does not change invalid-record handling.

ConfigSnapshot has no asset_type. Phase 7 therefore partitions by **UTC year,
month, day**, without inventing type from identifiers or requiring an asset join.
Asset-type partitioning can follow explicit registry enrichment. There is no
asset_id partition, avoiding one tiny directory per asset.

Raw date comes from Kafka timestamp, since malformed payloads may have no usable
event time. Normalized snapshots/diffs use snapshot event time; alerts use
created_at. Diagnostics use event time or their safe Kafka timestamp, falling back
to 1970-01-01 when neither exists. Spark SQL uses UTC regardless of host timezone.
Normalized snapshots and Alert payloads retain microseconds; the existing state
output's additional diff/diagnostic event-time column has millisecond precision.

Example object path:

```text
config-lake/processed/snapshots/
  query_id=<Spark checkpoint UUID>/batch_id=00000000000000000001/
    year=2026/month=10/day=5/part-....snappy.parquet
```

The query/batch namespace provides a bounded rewrite target for retry; date
partitions allow Spark partition pruning. Files use Snappy compression, with a
configurable maximum 100,000 rows/file. Small micro-batches still create small
files; compaction and a catalogue are later work, not a measured tuning result.

## Commit and recovery contract

The local Spark checkpoint's `metadata` file supplies its persistent query UUID.
It stays stable across process restart and changes for a fresh checkpoint. The
lake writer never creates an independent authoritative state database.

For each nonempty batch:

1. Look for `_commits/<query UUID>/<zero-padded batch ID>.json`.
2. If committed, verify format, identity, and record counts, then preserve files.
3. Otherwise write each nonempty dataset into its deterministic batch directory.
   Overwrite applies only to that uncommitted batch, never an entire dataset.
4. Publish one manifest containing dataset paths and record counts, using a
   conditional S3 put that rejects overwriting an existing manifest.
5. Continue to optional derived search indexing, then the existing Kafka alert
   sink, and return for Spark batch commit.

Parquet uses Hadoop's **S3A magic committer** via Spark's
PathOutputCommitProtocol/BindingParquetOutputCommitter, avoiding filesystem rename
assumptions for object-store task commits. See the
[Hadoop 3.4.1 committer documentation](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/committers.html).
Endpoint, region, path-style access and credentials are explicitly configured for
MinIO; see [third-party-store settings](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/third_party_stores.html).

`read_committed` reads **only manifest-listed batch directories** with typed Spark
schemas. A failed partial batch can leave visible S3 objects, but these are not
published history. Direct recursive reads of the dataset root bypass this contract.
The reader lists small manifests on the driver; Parquet data remains distributed.

One writer must own each checkpoint. Stable source identities, code, rules, and
state configuration are required on recovery; count checks are not a byte-level
replay verifier. Concurrent writers and automatic schema/state migration are out
of scope. Deleting a checkpoint creates a new query namespace and can replay the
same logical IDs; deduplicate across independent runs by snapshot_id/diff_id/alert_id
(or Kafka provenance for raw records) when constructing a later consolidated table.

There is **no atomic transaction across MinIO, Elasticsearch and Kafka**, and no end-to-end
exactly-once claim. A failure after lake commit reuses its files on retry; a
failure after Kafka delivery may still duplicate physical alerts. Sink exceptions
fail the query visibly. Protect both the checkpoint and bucket for recovery.

## Configuration and commands

`CONFIGSTREAM_LAKE_ENABLED` defaults to `false`. Lake mode chooses a new default
checkpoint `.cache/spark-checkpoints/config-snapshots-v7`; never reuse a v6 or
parse-only checkpoint. Existing checkpoint offsets still override startingOffsets.

Other settings: `MINIO_ENDPOINT=http://localhost:9000`, `MINIO_REGION=us-east-1`,
`CONFIGSTREAM_LAKE_BUCKET=config-lake`, optional safe relative
`CONFIGSTREAM_LAKE_PREFIX`, and `CONFIGSTREAM_LAKE_MAX_RECORDS_PER_FILE=100000`.
Credentials are required from `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, with no
hard-coded fallback. They are omitted from config representations and logs.

```sh
make lake-setup
make lake-env
set -a; . .cache/minio.env; set +a
make lake-up
make kafka-topics
make lake-init
```

`make lake-env` creates random development credentials with mode 0600 and never
overwrites an existing file. It prints only the path/load command. Keep the same
credentials when restarting the existing volume. These root credentials and
plaintext transport are for loopback-only local development.

Terminal A, with credentials exported and an unused checkpoint:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase7-example make lake-stream
```

After the initial progress log, terminal B (export credentials there too):

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase7-example'
```

Expect three raw and normalized snapshots, two adjacent diffs, and two alerts.
Inspection prints counts only, not raw secrets:

```sh
make lake-inspect
```

Optionally restrict inspection with `LAKE_ARGS='--query-id <query UUID>'`. Counts
otherwise include all committed query namespaces in the configured prefix.
Stop the live query before running acceptance tests:

```sh
make check
make spark-test
make integration-test
make spark-integration-test
make lake-integration-test
make lake-down
```

`make lake-down` stops both development services while preserving volumes. Regular
Kafka commands do not require MinIO credentials. `.env.example` is documentation;
Python does not implicitly load it. `make setup`/`make spark-setup` omit the lake
extra, so use `make lake-setup` before lake commands after resyncing dependencies.

## Recorded local acceptance

On 2026-10-08, the macOS arm64 local environment passed all **654 tests**:
`make check` (599 unit tests, Ruff lint and formatting), `make spark-test`
(48), `make integration-test` (3), `make spark-integration-test` (3), and
`make lake-integration-test` (1). These are correctness checks, not throughput
benchmarks. The lake integration verified partial failure/retry, checkpoint
recovery, unchanged committed Parquet ETags, and schema-valid keyed alerts.

A separate live query used a fresh checkpoint and prefix:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase7-live-20261008 \
CONFIGSTREAM_LAKE_PREFIX=phase7-live-20261008 make lake-stream
```

After startup, another terminal published:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase7-live-router --start-time 2026-10-08T00:00:00Z'
.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka --start-time 2026-10-08T00:00:00Z
.venv/bin/python -m scripts.kafka_invalid_demo
```

Committed inspection returned **15 raw records, 11 normalized snapshots, 8 diffs,
5 alerts, and 4 invalid-record diagnostics**. This combines three out-of-order
demo snapshots, eight generator snapshots, and four intentionally invalid records.
The invalid records are retained only in raw and diagnostics; duplicate retention
is covered separately by the service-free and full lake integration tests.

On 2026-10-09, `make check` again passed 599 tests. Resuming the same checkpoint
with `SPARK_STARTING_OFFSETS=earliest` and `make lake-stream
SPARK_ARGS=--available-now` processed zero new input rows and stopped normally.
Inspection retained exactly the same counts, demonstrating that checkpointed
offsets override a changed starting-offset setting. Credentials were exported
from the private ignored `.cache/minio.env` file for all lake commands.

## Validation and limits

Service-free tests cover raw binary preservation, typed projections, UTC/date
partitions, partial-write invisibility, overwrite retry, immutable committed
replay, and manifest path validation. Full integration tests use Kafka, Spark and
MinIO, intentional out-of-order/duplicate/conflicting/invalid input, and separate
Spark processes. They inject a Kafka sink failure **after lake commit**, restart
with the same checkpoint, verify retained Parquet ETags, consume keyed alerts,
and then extend the history with a fourth version.

Only the snapshot topic is archived; assets/events are still independent Kafka
streams. URI content is preserved as a reference in raw envelopes, not fetched.
All configuration remains potentially sensitive: raw contains original secrets;
normalized masking is conservative, not universal. Restrict bucket, volume,
checkpoints, credentials and backups. There is no encryption-at-rest configuration,
production HA, lifecycle cleanup, compaction, table format, or performance claim.
Phase 8 now supplies [safe Elasticsearch projections](elasticsearch.md). They can
be deleted and rebuilt with `read_committed`; the lake remains authoritative.
Historical batch analytics come later.
