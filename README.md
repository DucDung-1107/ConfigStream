# ConfigStream

**Scalable Configuration Change Storage and Analytics Platform**

ConfigStream is a university Big Data Storage and Processing project for storing,
comparing, and analyzing configuration changes over time. Configuration history
should answer not just *what changed*, but *when, how often, and with what risk*.

The central entity is an **Asset**: anything that owns configuration. Network
devices provide the primary demo, but nginx servers, Linux servers, applications,
and Kubernetes resources fit the same model. This is a data-pipeline project, not
a network automation framework or a frontend-first application.

## Current status

Phase 20 is a **conditional comparative evaluation**, with accepted bounded
matched pilots and explicit unexecuted full-rate cells; see the
[evaluation acceptance](docs/phase20-acceptance.md). Terraform, RPKI, RIS and DNS
retain distinct temporal roles. Phase 20K.1 has passed real single-node Kubernetes
application acceptance: Kafka → native Spark driver/executor → MinIO, batch
readback, Elasticsearch/FastAPI and owned-fixture recovery. See
[Kubernetes acceptance](docs/phase20-k8s-acceptance.md). This is not HA or full-data replay.
The [Phase 20V dashboard](docs/phase20v-acceptance.md) implements the actual
shadcn/ui dashboard-01 reference and runs on K3s with verified aggregates and
read-only configuration metadata. Desktop/tablet/mobile screenshots,19 browser
tests and an actual configuration/API rehearsal passed; access through
`kubectl port-forward -n cs-app svc/dashboard 13080:8080 --address 127.0.0.1`.
See the [seven-step demo](docs/phase20v-demo-script.md). Phase 21 has not begun.

### Historical Phase 13 baseline

Phase 13 Round 1 adds a [23-source feasibility survey](docs/phase13-dataset-survey.md),
[bounded real-data measurements](docs/phase13-feasibility-methodology.md), and a
[conditional Terraform/RPKI/DNS/RIS shortlist](docs/phase13-shortlist-decision.md).
Routing events and observed state are **not configuration-file edits**. No new
source adapter or pipeline integration is implemented; the Phase 12 empirical
conclusion remains unchanged.

**Phases 0–12 complete.** Pydantic contracts,
a deterministic stateful generator, JSONL output, and a local single-broker Kafka
path with an explicit asset registry and automated CI are available.
Spark reads `config.snapshots`, parses/validates, applies event-time deduplication,
and uses checkpointed native per-asset version buffers. It reuses pure Python
normalization/diff logic and YAML rules to publish deterministic Alerts to Kafka.
Pairing uses **asset_id + adjacent versions**, not previous-row arrival order.
Optional **MinIO/Parquet history is authoritative**. Phase 8 adds Elasticsearch
as a rebuildable metadata-only search projection. Phase 9 adds a thin read-only
FastAPI metadata API. The current research dashboard and Kubernetes acceptance are described above. Invalid input
produces safe diagnostics, not a DLQ.

Phase 10 adds [public baseline provenance](docs/datasets.md), controlled
semi-synthetic histories, [historical Spark analytics](docs/historical-analytics.md)
and a [measured replay harness](docs/benchmark.md). The
[acceptance record](docs/phase10-acceptance.md) includes the completed one-million
snapshot local run; do not interpret generated histories as observed production data.

Phase 11 adds [observed public Git history](docs/real-history-sources.md),
[empirical workload calibration](docs/workload-calibration.md) and an
[external-validity assessment](docs/external-validity.md). ConfigStream now uses
three evidence layers: observed public file histories, calibrated/semi-synthetic
controlled histories, and stress workloads. **Git history is a public proxy for
configuration evolution, not device deployment frequency.**

The [Phase 11 acceptance record](docs/phase11-acceptance.md) includes real-history
replay, a 100518-snapshot calibrated run, committed-history analytics and regression.

Phase 12 adds [TerraDS v2](docs/terrads.md): a measured **1513378 current Terraform
files / 2.60 GB source corpus**, followed by a fixed 500-repository history sample.
Accepted observed history contains **6505 file incarnations / 10188 revisions /
12.28 MB**; all 3683 adjacent pairs were verified through Kafka–Spark–MinIO and
historical analytics, with a real Terraform Elasticsearch/FastAPI smoke.
**Decision B:** [empirical acceptance](docs/phase12-acceptance.md) supports an
architecture-evaluation framing, not naturally high-frequency telemetry.
77.48% of accepted files have one version. Population estimates are inferred,
not additional observed data. See [reproduction](docs/phase12-methodology.md).

The expanded acceptance audit preserves the original sample and replay, adds
full static repository measurements, persisted adjacency ground truth and funnel
plots, and verifies a fresh real-history replay with state-cap headroom. It does
not multiply observed history or change the architecture-evaluation conclusion.

| Dataset | Observed? | Assets | Versions | Content bytes | Purpose |
| --- | --- | ---: | ---: | ---: | --- |
| Batfish public baseline | Real files, baseline only | 324 | Baseline only | 361177 | Syntax |
| FRRouting selected Git history | Yes, test-file history | 1900 incarnations | 2516 | 1109458 | Observed evolution |
| NGINX selected Git history | Yes, shipped templates | 6 | 9 | 24540 | Cross-domain |
| observed-calibrated ×40 | No, cloned histories | 76240 | 100518 | 45002724 | Controlled scaling |
| Phase 10 stress-replay | No, semi-synthetic | 10000 | 1000000 | 1144675896 | Capacity |
| TerraDS v2 static | Real current source, not temporal | 82749 repos / 1513378 files | Current files only | 2601501820 | Static scale/variety |
| TerraDS Git sample | Yes, accepted file histories | 375 repos / 6505 incarnations | 10188 | 12278085 | Observed IaC evolution |

Observed public history is sparse/small; the project is best framed as a scalable
data-pipeline evaluation with empirically characterized configuration workloads,
not proof of naturally massive per-device configuration telemetry. Use
`make observed-fetch`, `make observed-inspect`, `make empirical-report`,
`make observed-replay`, `make calibration-generate` and `make phase11-integration-test`.

**The high input rates represent accelerated replay/stress testing of accumulated
fleet configuration history, not expected per-device configuration frequency.**

```text
Public configurations -> controlled histories -> Kafka -> Spark streaming
                                                          ├─> Kafka alerts
                                                          ├─> Elasticsearch -> FastAPI
                                                          └─> MinIO -> Spark batch -> analytics/
```

`make dataset-fetch`, `make dataset-inspect`, `make workload-generate`,
`make analytics-run`, `make analytics-inspect`, `make benchmark-smoke`,
`make benchmark-main` and `make benchmark-report` operate from the project root.
Downloaded/generated inputs stay ignored; setup does not download the corpus.

Implemented Kafka path (all keys are `asset_id`):

```text
Generator
  ├─> config.assets       (startup registry)
  ├─> config.events       (audit / operational history)
  └─> config.snapshots ─> Spark parse / validate ─> watermark + dedup
       ─> per-asset version state ─> normalize + diff ─> YAML rules
       ─> Alert ─> Kafka config.alerts
```

With optional storage/search enabled, the same stateful output boundary writes:

```text
Spark state / normalize / diff / rules
  ├─> MinIO / committed Parquet     (authoritative history, written first)
  ├─> Elasticsearch ─> FastAPI      (safe derived metadata, read-only HTTP)
  └─> Kafka config.alerts           (Alert envelopes)
```

Elasticsearch is **not the source of truth**; raw configuration, full normalized
content and diff lines are excluded from search. No atomic multi-sink transaction.

## Read-only API / end-to-end MVP

FastAPI **0.143.0** and Uvicorn **0.54.0** run on the host at
**127.0.0.1:8000**, unauthenticated for local development only. Do not expose
publicly. The API needs only Elasticsearch at runtime, not Java/Kafka/MinIO.
It uses the existing search abstraction, validates safe Pydantic response models,
and never returns config bodies or scans Parquet for normal requests.

```sh
make api-setup
# With the Phase 8 search stack initialized and populated:
make api-run
```

In another terminal:

```sh
curl -s http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/readyz
curl -s 'http://127.0.0.1:8000/api/v1/assets/phase9-router/latest-snapshot'
curl -s 'http://127.0.0.1:8000/api/v1/diffs?asset_id=phase9-router'
curl -s 'http://127.0.0.1:8000/api/v1/alerts?asset_id=phase9-router&severity=critical'
```

Business routes use `/api/v1`: snapshots, diffs, alerts, diagnostics, latest
snapshot and bounded alert/hourly-diff statistics. Lists use `search_after`
cursors (default 50, max 200), timestamp plus stable ID sorting, and no exact
global count. Latest snapshot uses version, not arrival order. `/healthz` checks
liveness; `/readyz` requires Elasticsearch and its aliases. OpenAPI is at `/docs`.
Set `CONFIGSTREAM_API_DOCS=false` to disable docs, redoc and OpenAPI. CORS is off.

The real out-of-order demo **v1, v3, v2** returned latest version **3**, only
**v1→v2 / v2→v3** diffs, and **logging-disabled / telnet-enabled** alerts. Stopping
Elasticsearch kept healthz=200 while readiness/business queries returned 503.
Restoring it and rebuilding twice from committed Parquet recovered the same
HTTP-visible records without restarting the API. See [API setup, full demo,
contracts and recorded acceptance](docs/api.md). No exactly-once or throughput claim.

## Architecture overview (long-term target)

1. Stateful synthetic, file, and optional SSH sources feed a separate collector.
2. The collector publishes asset-keyed events and snapshots to Kafka.
3. Spark Structured Streaming validates, deduplicates, normalizes, computes diffs,
   evaluates rules, and aggregates changes using event time.
4. MinIO holds raw and processed Parquet history; Elasticsearch serves searchable
   metadata, events, alerts, and metrics.
5. Spark batch reads historical Parquet for analytics. A thin FastAPI application
   reads Elasticsearch; a minimal dashboard may follow.

See [architecture](docs/architecture.md) and [data model](docs/data-model.md).

## Quick start

Prerequisites: Git, Make, an existing Python 3 installation with `venv` and `pip`,
and internet access for the initial setup. JSONL and unit tests need no services.
Kafka mode additionally needs a running Docker Engine and Docker Compose v2+
(Docker Desktop or another compatible runtime on macOS; Docker Engine on Linux).

```sh
git clone https://github.com/kongwoang/ConfigStream.git
cd ConfigStream
make setup
make check
```

`make setup` installs uv in `.tools/uv`, obtains Python **3.12** if necessary, and
creates `.venv` using committed `uv.lock`. Downloaded Python builds, caches, and
temporary files stay inside this repository (`.tools/`, `.cache/`). It does not
install global tools or edit shell profiles. These generated directories are
ignored by Git. Setup can be repeated safely.

Setup includes the optional `kafka` dependency for the quickstart. The installed
library's base dependencies remain sufficient for JSONL; Kafka imports are lazy.

| Command | Purpose |
| --- | --- |
| `make setup` | Install the locked development environment |
| `make test` | Run local tests without external services |
| `make lint` | Check Ruff lint and formatting |
| `make format` | Apply Ruff fixes and formatting |
| `make check` | Run lint, formatting checks, and tests |
| `make generator` | Print 10 changes (20 JSONL records) across 5 assets, seed 42, without sleeping |
| `make processing-demo` | Normalize router-001 v10/v11 and print masked configurations, hashes, and a deterministic ConfigDiff |
| `make kafka-up` / `make kafka-down` | Start a healthy broker / stop it, preserving its volume |
| `make kafka-topics` | Create missing topics and verify their explicit configuration |
| `make kafka-demo` | Publish 3 assets and 8 changes (19 records total) |
| `make kafka-status` | Show broker status |
| `make integration-test` | Run real Kafka tests, separately from unit tests |
| `make spark-setup` | Install locked Kafka + Spark extras; requires an external Java 21 JDK |
| `make spark-stream` | Run stateful snapshot processing and Kafka alert output |
| `make phase6-demo` | Publish v1, v3, v2; expect only adjacent diffs and two alerts |
| `make kafka-consume-alerts` | Print retained alert keys and envelopes |
| `make spark-test` | Run local[2] SQL/native state tests, without Kafka |
| `make spark-integration-test` | Verify Kafka → Spark → alerts, invalid input, and process restart |
| `make lake-setup` / `make lake-env` | Install locked lake dependencies / generate private local MinIO credentials |
| `make lake-up` / `make lake-down` | Build/start Kafka + MinIO / stop them, preserving volumes |
| `make lake-init` | Create the private lake bucket idempotently |
| `make lake-stream` | Run stateful streaming with raw/processed Parquet history |
| `make lake-inspect` | Count committed history without printing configuration bodies |
| `make lake-integration-test` | Verify Kafka → Spark → MinIO, sink failure, replay and restart |
| `make search-setup` / `make search-up` | Install locked search dependencies / start Kafka + MinIO + Elasticsearch |
| `make search-init` | Idempotently create/verify strict mappings and versioned-index aliases |
| `make search-stream` | Enable lake-first live metadata indexing, then Kafka alerts |
| `make search-inspect` / `make search-query` | Count and query safe derived documents |
| `make search-reindex` | Rebuild from committed Parquet using distributed bulk writes |
| `make search-integration-test` / `make search-down` | Run real recovery/rebuild tests / stop all services, preserving volumes |
| `make api-setup` / `make api-run` | Install locked full-demo extras / serve metadata on loopback |
| `make api-test` | Run service-free API/model/pagination/error tests |
| `make api-integration-test` | Verify real API→ES and Kafka→Spark→lake→ES→HTTP recovery |
| `make clean` | Remove test/build outputs, not datasets or environments |

[GitHub Actions CI](https://github.com/kongwoang/ConfigStream/actions/workflows/ci.yml)
runs on pushes to `main` and pull requests, using Python 3.12 on Ubuntu, locked
dependencies, `make check`, and optional service-free `make api-test` (no Spark).
A separate Java 21 job runs `make spark-test`.
Neither job starts Docker or runs Kafka/Spark integration tests. Dependency
downloads are cached. Run service-dependent tests separately as documented below.

`.env.example` documents Kafka environment variables. Defaults work locally;
Python does not automatically load `.env`. Export overrides in your shell, such as
`export KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Never commit credentials, generated
datasets, or service state. Kafka data is in a Docker-managed named volume, not Git.

## Kafka quickstart

```sh
make setup
make check
make kafka-up
make kafka-topics
make kafka-demo
make kafka-consume-assets KAFKA_CONSUME_MAX_MESSAGES=3
make kafka-consume-events KAFKA_CONSUME_MAX_MESSAGES=8
make kafka-consume-snapshots KAFKA_CONSUME_MAX_MESSAGES=8
make integration-test
make kafka-down
```

The broker is pinned to `apache/kafka:4.1.2`, with KRaft, one node, and an IPv4
loopback listener at `localhost:9092`. Its image supports Linux arm64 and amd64;
the local acceptance run uses Apple Silicon. This plaintext, replication-factor-1
setup is **not production-ready**. It does not install a container runtime for you.

```sh
.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka
```

Kafka mode sends one Asset per initialized asset, flushes that registry batch,
then sends changes. The three active topics receive **3 / 8 / 8** records for a
fresh demo. Re-running appends records; idempotence does not deduplicate independent
generator runs. Consumer commands print `key<TAB>JSON` and default to one message.

`--sink jsonl` remains the default with byte-compatible Phase 2 output; asset
records are only added in Kafka mode. Both transports use the same JSON envelope
and existing versioned schemas. Kafka logs go to stderr; Kafka mode emits no JSONL
to stdout and does not accept file output.

**Ordering is only within one partition of one topic.** Sending an event before
its snapshot is not a cross-topic delivery/consumption guarantee. Spark
processing consumes `config.snapshots`; `config.events` is an audit
stream, not a required ordered trigger. See [topics and failure semantics](docs/kafka-topics.md).

## Spark streaming quickstart

Install a **Java 21 JDK** separately; `java -version` must report major 21. Set
`JAVA_HOME` if needed. Spark/PySpark is pinned to **4.0.2**, Scala binary **2.13**,
and connector **`org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`**. The Python
entry point resolves that connector automatically; internet access is needed on
first launch. Kafka stays in Docker; Spark runs on the host, not in a new cluster.

```sh
java -version
make spark-setup
make check
make spark-test
```

Terminal A, from the project root:

```sh
make kafka-up
make kafka-topics
make spark-stream
```

Wait for the first `Progress` log (initial offsets established). In terminal B:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id demo-router'
make kafka-consume-alerts KAFKA_CONSUME_MAX_MESSAGES=2
```

Expect **two adjacent diffs: v1→v2 and v2→v3**, never v1→v3, and logging-disabled
(high) plus telnet-enabled (critical) Alert envelopes in `config.alerts`. Keys equal
asset_id. Assets/events are not consumed. One query combines stateful outputs and
Phase 4 invalid diagnostics; it logs safe summaries, not configuration bodies.
The summary sample is capped at 20 rows per batch; full counts are logged separately.
The consumer starts at retained history, so reused brokers may show older alerts.

To exercise malformed JSON, wrong record type, missing asset ID, and key mismatch:

```sh
.venv/bin/python -m scripts.kafka_invalid_demo
```

These are deliberately bad **local test records**. The live query stays running
and shows four diagnostics. Stop the live query before integration tests, which
write their own records/alerts:

```sh
make spark-integration-test
make kafka-down
```

Default `SPARK_STARTING_OFFSETS=latest` reads new data on a fresh query. For bounded
replay of retained history, use **a new checkpoint directory**:

```sh
SPARK_STARTING_OFFSETS=earliest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/replay-example \
make spark-stream SPARK_ARGS=--available-now
```

An existing checkpoint overrides starting offsets; reusing it resumes rather than
replaying. Checkpoints and downloaded JARs are ignored. See [Spark runtime, SQL
validation, progress, and acceptance](docs/spark-streaming.md). `make setup` syncs
only the base/Kafka environment; use `make spark-setup` again before Spark commands.

Phase 6 defaults to a new `.cache/spark-checkpoints/config-snapshots-v6` directory;
never reuse a Phase 4 console checkpoint. Defaults are a 10-minute event-time
watermark, 30-minute asset inactivity timeout, and 32 buffered versions per asset.
The demo uses fixed 2026-10-05 timestamps; the generator defaults to 2026-09-30.
Use aligned `--start-time` values or separate checkpoints when mixing workloads.
Rules come from `rules/default.yaml`. See [state, rules, replay, and delivery
semantics](docs/stateful-processing.md). Deterministic IDs do not make Kafka sink
retries end-to-end exactly-once; physical duplicate alerts remain possible.

## MinIO data-lake quickstart

Lake mode preserves original Kafka key/value bytes and provenance before
deduplication or late-event filtering. It stores accepted normalized snapshots,
ConfigDiffs, Alerts and safe diagnostics as typed Parquet. The existing query uses
one output boundary for the lake and Kafka alert sinks.

```sh
make lake-setup
make lake-env
set -a; . .cache/minio.env; set +a
make lake-up
make kafka-topics
make lake-init
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase7-example make lake-stream
```

Wait for initial progress, then in another terminal with credentials exported:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase7-example'
make lake-inspect
```

For this fresh workload expect **3 raw snapshots, 3 normalized snapshots, 2 diffs,
and 2 alerts**. Counts otherwise include earlier committed histories. Use
`LAKE_ARGS='--query-id <query UUID>'` to inspect one query. Stop the live stream
before `make lake-integration-test`; finish with `make lake-down`.

The default private `config-lake` bucket uses `raw/`, `processed/`, and the Phase 10
`analytics/` zone. History date partitions are UTC year/month/day, inside deterministic
query/batch directories; asset-type enrichment comes later. A commit manifest
publishes complete batches, so readers exclude partial writes and retries retain
committed files. Kafka delivery remains at-least-once; the two sinks are not atomic.

MinIO is built inside Docker from fixed release `RELEASE.2025-10-15T17-29-55Z` and
commit `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, with Go 1.24.9 and Alpine 3.22.2.
First build and S3A dependency downloads can take several minutes. The source-build
choice reflects the upstream community distribution; see [lake runtime, schemas,
security, committed reads and recovery](docs/data-lake.md). Ports bind to loopback;
credentials are random, mode 0600, and remain in ignored `.cache/minio.env`.

Lake mode defaults to a **fresh v7 checkpoint**. Keep it separate from Phase 6 and
parse-only checkpoints. `make setup` and `make spark-setup` do not include the lake
extra; rerun `make lake-setup` before lake commands if dependencies were resynced.

## Elasticsearch quickstart

Server **9.5.5** and official Python client **9.5.1** are pinned. Elasticsearch is
an optional loopback-only, unauthenticated local development service; no Kibana or
FastAPI is required.

```sh
make search-setup
make lake-env
set -a; . .cache/minio.env; set +a
make search-up
make kafka-topics
make lake-init
make search-init
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase8-example \
CONFIGSTREAM_LAKE_PREFIX=phase8-example make search-stream
```

After initial query progress, in another terminal:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase8-router --start-time 2026-10-11T00:00:00Z'
make search-inspect
make search-query SEARCH_ARGS='snapshots --asset-id phase8-router --latest'
make search-query SEARCH_ARGS='diffs --asset-id phase8-router'
make search-query SEARCH_ARGS='alerts --group-by severity'
```

Empty indices should contain **3 snapshots, 2 adjacent diffs, 2 alerts**, with
latest snapshot version 3. Stop the query before rebuilding:

```sh
CONFIGSTREAM_LAKE_PREFIX=phase8-example make search-reindex
CONFIGSTREAM_LAKE_PREFIX=phase8-example make search-reindex
make search-inspect
make search-down
```

Reindex replaces documents by deterministic ID, so a second pass does not increase
logical counts. To prove a complete rebuild, intentionally remove only your four
development indices and run `search-init` first; never remove MinIO history.
See [mappings, safe fields, query examples, bulk retries and recovery](docs/elasticsearch.md).
FastAPI remains Phase 9; the current query helpers use stable aliases directly.

## Try the schemas

After setup, this example constructs and validates an inline snapshot:

```sh
.venv/bin/python - <<'PY'
from datetime import UTC, datetime
from hashlib import sha256

from schemas import ConfigSnapshot

content = "hostname router-001\nlogging enabled\n"
snapshot = ConfigSnapshot(
    snapshot_id="snap-001",
    asset_id="router-001",
    version=1,
    event_time=datetime(2026, 9, 30, 3, 0, tzinfo=UTC),
    ingest_time=datetime.now(UTC),
    hash=sha256(content.encode("utf-8")).hexdigest(),
    content=content,
)
print(snapshot.model_dump_json(indent=2))
assert ConfigSnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot
PY
```

All five entities emit `schema_version: "1.0"`. Timestamp inputs require a timezone
and are converted to UTC. A snapshot has exactly one of `content` or `content_uri`;
an inline snapshot's SHA-256 is checked against its original UTF-8 content. Empty
configuration is valid. Schema validation does not normalize or mask secrets.

Example change event (independent of the snapshot topic):

```json
{
  "schema_version": "1.0",
  "event_id": "evt-001",
  "asset_id": "router-001",
  "event_type": "config_changed",
  "source": "synthetic",
  "event_time": "2026-09-30T03:00:00Z",
  "ingest_time": "2026-09-30T03:00:01Z",
  "metadata": {"snapshot_id": "snap-001"}
}
```

The Phase 2 generator uses this contract and includes mutation details in event
metadata. See [data-model semantics and limitations](docs/data-model.md) before
writing producers or consumers. Tests exercise invalid inputs, generic assets, JSON
round-trips, timestamp handling, content locations/hashes, diff consistency, and
severity values. They are schema tests, not pipeline integration or performance tests.

## Generate a stateful workload

```sh
make generator
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42
.venv/bin/python -m generator.main --assets 100 --events 1000 --rate 100 --seed 42 --no-sleep
.venv/bin/python -m generator.main --assets 1000 --duration 30 --rate 100 --seed 42 --no-sleep --output datasets/sample.jsonl
```

Each asset owns a separate configuration and version counter. Mutations transform
its previous state rather than creating unrelated snapshots. Built-in types are
`network_device`, `nginx_server`, and `generic_service`. An internal template is
version 0 (not emitted); the first mutation emits version 1. Later changes link to
the preceding emitted snapshot for that asset.

Every change writes **ChangeEvent first, ConfigSnapshot second**, one envelope per
line. For `--assets 3 --events 8 --seed 42`, the first output line is:

```json
{"payload":{"asset_id":"service-000003","event_id":"evt-73e40c41-486c-596a-ba0a-cd52d958a7dd","event_time":"2026-09-30T00:00:00Z","event_type":"config_changed","ingest_time":"2026-09-30T00:00:00.005000Z","metadata":{"asset_type":"generic_service","mutation":"change_scalar","new_value":"15","old_value":"30","previous_snapshot_id":null,"sequence":0,"setting":"timeout","snapshot_id":"snap-d97a8487-9359-5f8b-840a-1054a6ccb455"},"schema_version":"1.0","source":"synthetic"},"record_type":"change_event"}
```

The following snapshot contains `timeout 15` instead of the template's `timeout 30`.
`--rate` means changes per second, not JSONL lines. Real-time pacing is enabled by
default; `--no-sleep` removes waiting without altering record contents. Duration
produces `floor(duration * rate)` changes, not a nondeterministic wall-clock cutoff.

The same seed and arguments produce byte-identical output, including IDs, hashes,
and timestamps. Defaults are seed **42**, rate **100**, and start time
**2026-09-30T00:00:00Z**; simulated ingestion is 5 ms later. Custom timezone-aware
`--start-time` and comma-separated `--asset-mix` are supported. Stdout is the
default; file output creates parent directories but refuses to overwrite existing
files. Generated `.jsonl` files and `datasets/` are ignored by Git.

See [generator design, CLI, verification, and limitations](docs/generator.md).

## Normalize and compare configurations

```sh
make processing-demo
```

The offline demo compares two snapshots of router-001, versions 10 and 11.
It masks passwords and removes generated timestamp comments before producing:

```diff
- logging enabled
+ logging disabled
- telnet disabled
+ telnet enabled
```

The existing `ConfigDiff` has `added=2`, `removed=2`, `changed=2`; replacements
are a subset of additions/removals, not extra changes. The command prints both
normalized hashes and the full model. Secret-only value rotations are deliberately
invisible in this diff, but source snapshots and raw hashes remain unchanged.

Use `processing.process_snapshot_pair(previous, current)` with validated
ConfigSnapshots, or normalize each once and call `compare_snapshots`. Same asset
and strictly increasing versions are required. Gaps are allowed and reported
separately in `result.version_gap`. Text/inline content only; no remote fetching.

Core processing tests need neither Kafka nor Spark. Phase 6 reuses this logic
inside native Spark state processing, selecting only adjacent versions (unlike the
offline API, which permits gaps). Normalization is not a universal secret detector.
See [exact rules, hash semantics, version pairing, and limitations](docs/config-processing.md).

## Initial roadmap

| Phase | Deliverable |
| --- | --- |
| 0 — complete | Bootstrap, architecture, tooling, packaging smoke test |
| 1 — complete | Pydantic Asset, ConfigSnapshot, ChangeEvent, ConfigDiff, Alert and tests |
| 2 — complete | Stateful, seeded synthetic workload generator with JSONL output |
| 3 — complete | Single-broker Kafka, asset registry, producer tests, GitHub Actions quality checks |
| 4 — complete | Direct Spark Kafka source, envelope parsing, validation, diagnostics, checkpoints |
| 5 — complete | Pure text normalization/masking, normalized hashes, deterministic explicit-pair diff |
| 6 — complete | Native Spark version state, watermark/dedup, YAML rules, Kafka alerts, safe progress metrics |
| 7 — complete | MinIO/Parquet raw and processed history, committed reads, retry/restart acceptance |
| 8 — complete | Safe Elasticsearch projections/aliases, distributed bulk indexing, committed-lake rebuild |
| 9 — complete | Read-only metadata FastAPI, bounded cursors, safe errors, HTTP end-to-end/rebuild acceptance |
| 10 — complete | Public configuration baselines, controlled histories, historical Spark analytics, measured local replay |
| 11 — complete | Observed public Git histories, empirical calibration, external-validity assessment |
| Subsequent research phases | See current status: four evidence roles, bounded evaluation, Kubernetes and dashboard; no Phase21 |

Python 3.12, Pydantic, pytest, Ruff, Kafka, optional confluent-kafka, and PySpark 4.0.2
form the current stack alongside optional MinIO/Parquet history and Elasticsearch
search and a thin read-only FastAPI serving layer.
Each phase is tested, committed, and pushed before the next begins. Recorded local
replay measurements do not establish production throughput, distributed-cluster
scalability, or end-to-end exactly-once delivery.

## Research evaluation status

Terraform configuration histories, scoped RPKI states, RIS native occurrences
and uncertain DNS measurements retain distinct semantics and accepted evidence.
[Phase20 acceptance](docs/phase20-acceptance.md) is **CONDITIONAL PASS**:
matched RPKI representations and27 three-repeat normalized-core engine trials
passed; full production comparisons and paced RIS capacity remain unexecuted.
Source ledgers stay private, public CI uses owned fixtures. Kubernetes20K.1 is a
separate accepted single-node owned-fixture deployment. The Phase20V dashboard
preserves accepted source scopes, units and uncertainty in a read-only presentation.
Do not begin Phase21 automatically.
