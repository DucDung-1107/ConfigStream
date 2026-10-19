# Read-only serving API (Phase 9)

## Purpose and boundaries

FastAPI serves **safe metadata** from the four existing Elasticsearch aliases.
It never opens a Spark session, consumes Kafka, scans Parquet, fetches a content
URI, or performs normalization, diffing, rules or version pairing. MinIO's
committed Parquet remains authoritative; Elasticsearch is rebuildable derived
state. The API holds no authoritative result cache. Ingestion may be stopped
while historical search remains available.

No configuration bodies, normalized_content, raw Kafka payloads or diff lines
are returned. There are no write/delete endpoints or raw-config downloads.
Phase 5 masking is not a universal secret detector. IDs and rule messages are
source-controlled metadata, not a general sanitization guarantee for arbitrary
text inserted by a privileged writer.

## Runtime and client choice

Python 3.12 and Pydantic v2 are unchanged. Optional `api` dependencies pin
**FastAPI 0.143.0**, **Uvicorn 0.54.0**, and **HTTPX 0.28.1** for TestClient.
Versions were checked against official [FastAPI release notes](https://fastapi.tiangolo.com/release-notes/)
and [Uvicorn release notes](https://uvicorn.dev/release-notes/), plus their published
Python/Pydantic dependency metadata. The lockfile pins transitive dependencies.
The current Starlette TestClient supports HTTPX with a deprecation warning;
this is not suppressed and does not affect the application transport.

Elasticsearch's [async client](https://www.elastic.co/docs/reference/elasticsearch/clients/python/async)
is supported, but this implementation deliberately reuses Phase 8's synchronous
9.5.1 client and query abstraction. All backend-facing routes use normal `def`
handlers, executed in FastAPI's [external threadpool](https://fastapi.tiangolo.com/async/),
not blocking I/O inside async request handlers. One thread-safe pooled client
is created per app lifespan and closed at shutdown, with the existing bounded
search timeout and **zero client retries**. No client is constructed per request.
This is a local MVP, not a concurrency/latency benchmark or production claim.

`create_app(config=None, service=None)` supports injecting a fake SearchService.
Lifespan constructs the client without connecting, so Elasticsearch outages do
not prevent startup or liveness. Startup never creates indices or writes data.

## Setup and configuration

```sh
make api-setup
make api-run
# Equivalent runtime entry point: .venv/bin/python -m api.main
# Or: .venv/bin/uvicorn api.app:create_app --factory --host 127.0.0.1 --port 8000
```

`api-setup` installs all extras for the full local pipeline. API-only development
can use the project-local uv with `sync --locked --extra search --extra api`,
exporting the Makefile's project-local cache/tool/temp settings first. API tests
need neither Java nor Spark nor services. Base `make setup` and `make check` remain
independent of API dependencies; setup synchronizes only its selected extras.

| Variable | Default |
| --- | --- |
| CONFIGSTREAM_API_HOST | 127.0.0.1 |
| CONFIGSTREAM_API_PORT | 8000 |
| CONFIGSTREAM_API_LOG_LEVEL | info |
| CONFIGSTREAM_API_DOCS | true |

The API reuses SearchConfig's ELASTICSEARCH_URL, index prefix, timeout and optional
paired username/password settings. It does not require CONFIGSTREAM_SEARCH_ENABLED
(that flag controls Spark indexing). Environment variables are not read in routes;
`.env` files are not loaded automatically.

Default host exposure is loopback-only and unauthenticated. **Do not expose this
setup publicly**. Authentication, deployment hardening and rate limits are future
work. CORS is disabled. The supported runner disables proxy trust and the server
header; logs contain method, route template, status and elapsed_ms, not query
values, document bodies, backend URLs or credentials.

## Endpoints

All endpoints are GET. Unsupported mutation methods return 405.

| Endpoint | Response / behavior |
| --- | --- |
| /healthz | Process liveness, always 200 while serving |
| /readyz | Elasticsearch reachable and all four aliases exist; 200 or 503 |
| /api/v1/snapshots | Paginated safe snapshot metadata |
| /api/v1/snapshots/{snapshot_id} | One snapshot, or 404 |
| /api/v1/assets/{asset_id}/latest-snapshot | Highest version for the asset, or 404 |
| /api/v1/diffs | Paginated diff summaries, never full lines |
| /api/v1/diffs/{diff_id} | One diff summary, or 404 |
| /api/v1/alerts | Paginated existing Alert semantics |
| /api/v1/alerts/{alert_id} | One alert, or 404 |
| /api/v1/diagnostics | Paginated safe diagnostic metadata |
| /api/v1/diagnostics/{diagnostic_id} | One diagnostic, or 404 |
| /api/v1/stats/alerts | Top 100 severity and rule_id buckets each |
| /api/v1/stats/diffs | UTC hourly diff counts, maximum 31-day range |

Readiness checks aliases but does not mutate/initialize mappings or disclose the
server version. Use `make search-init` for Phase 8's strict mapping verification.

All lists accept asset_id, start, end, end_before, limit and cursor. Alerts additionally accept
severity (the existing info/low/medium/high/critical enum) and rule_id. Diagnostics
accept `type`, mapped internally to diagnostic_type. Stats accept asset_id/start/end.
Unknown query parameters are rejected; no raw DSL, scripts, sort expressions or
arbitrary field names are accepted.

Limits: default 50, minimum 1, maximum 200. ID/filter strings are nonblank and at
most 1024 characters without introducing a vendor-specific identifier regex.
Cursor strings are at most 16384 characters (including maximum-length Unicode IDs).
These are public request bounds, not
changes to the underlying domain schema. Existing generated IDs are compatible.

Time bounds require timezone-aware ISO 8601, for example `2026-10-11T00:00:00Z`
or `2026-10-11T07:00:00+07:00`. Naive dates, numeric epochs and invalid formats are
rejected. `start` and `end` are inclusive and normalized to UTC; start > end is rejected.
Lists additionally support exclusive `end_before`, mutually exclusive with `end`,
and require start < end_before. Calendar-date clients use the following UTC midnight
as `end_before` so fractional seconds at the end of the selected day remain included.
Response timestamps are ISO 8601 UTC with `Z` (Python datetime microsecond
precision). Hourly stats default to the last 24 hours, ending now if end is absent;
explicit start/end spans over 31 days are rejected before querying. Top terms
are bounded, not exhaustive distinct-value enumeration.

## Response contracts

Pydantic v2 validates every returned source document. Unexpected fields or malformed
required fields fail the entire request safely instead of dropping rows silently.
Raw Elasticsearch internals such as _index, _score and shard metadata never escape.

- Snapshot: snapshot_id, asset_id, version, event_time, ingest_time, raw_hash,
  normalized_hash, normalization_version.
- Diff: schema_version, diff_id, asset_id, old_snapshot_id, new_snapshot_id,
  added, removed, changed, event_time.
- Alert: schema_version, alert_id, asset_id, snapshot_id, rule_id, severity,
  message, created_at. No wall-clock timestamp is substituted by the API.
- Diagnostic: diagnostic_id/type, reason, optional asset/snapshot/version(s),
  event_time, missing range, safe Kafka coordinates/timestamps, watermark_ms,
  event_time_ms. No original malformed record is returned.

Lists return `{items, count, has_more, next_cursor}`. `count` is the number on this
page, **not** an exact global total. Alert stats return by_severity/by_rule_id lists
of `{key, count}`. Diff stats return interval=1h and `{time, count}` buckets.

## Cursor pagination

The search layer fetches limit+1 hits with `track_total_hits=false` and no deep
`from` pagination. It returns at most limit items; the extra hit sets has_more.
The next cursor contains the **last returned** hit's native sort values.

Sorts are event_time DESC + snapshot_id/diff_id/diagnostic_id ASC; alerts use
created_at DESC + alert_id ASC. Latest snapshot is version DESC + snapshot_id ASC,
not arrival or ingestion time. Elasticsearch's date_nanos native sort values
retain timestamp precision even though response datetimes use microseconds.

Cursors are URL-safe base64 JSON with validated bounded scalar sort values and
a SHA-256 context binding API policy, document kind and normalized filters.
Changing the limit is allowed; changing filters or endpoint requires a fresh
cursor. No pickle, client-defined DSL or sort is evaluated. Cursors are opaque,
not encrypted/signed secrets or authorization tokens.

No Point-in-Time snapshot is held. Pagination is deterministic for unchanged
indices, including identical timestamps, but concurrent indexing, updates,
deletion or an alias rebuild can change a multi-page view. Start again after a
rebuild. A stable historical export/PIT lifecycle is future work, not promised here.

## Error policy

All handled errors use `{"error":{"code":"...","message":"..."}}`.
No backend exception body, URL, credential, traceback or offending document is
returned/logged. Request-validation responses do not echo user input.

| Condition | HTTP | Code |
| --- | --- | --- |
| Invalid/bounded request parameters or time range | 422 | invalid_request |
| Malformed or query-incompatible cursor | 400 | invalid_cursor |
| Unknown document / asset has no snapshot | 404 | not_found |
| Connection failure, missing alias/index, backend overload | 503 | search_unavailable |
| Backend timeout | 504 | search_timeout |
| Invalid source/projection | 502 | invalid_search_response |
| Other backend failure | 502 | search_failed |
| Unknown route / unsupported method | 404 / 405 | http_error |

Readiness always maps backend failure to 503; liveness remains 200. Normal client
connection recovery does not require an API restart. Sink retry/atomicity semantics
remain Phase 8's; the API does not change them or imply exactly-once guarantees.

## Full local demo

Load ignored MinIO credentials as described in [data-lake.md](data-lake.md).
Use unused matching lake/index/checkpoint prefixes; do not run other publishers
or integration suites concurrently with a live demo on the same snapshot topic.

```sh
make api-setup
make search-up
make kafka-topics
make lake-init
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo make search-init
```

Terminal B:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase9-demo \
CONFIGSTREAM_LAKE_PREFIX=phase9-demo \
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo make search-stream
```

After Spark reports the query started and initialized its initial Kafka offsets,
Terminal C:

```sh
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo make api-run
```

Terminal D:

```sh
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase9-router --start-time 2026-10-11T00:00:00Z'
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo make search-inspect
curl -s http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/readyz
curl -s 'http://127.0.0.1:8000/api/v1/assets/phase9-router/latest-snapshot'
curl -s 'http://127.0.0.1:8000/api/v1/diffs?asset_id=phase9-router'
curl -s 'http://127.0.0.1:8000/api/v1/alerts?asset_id=phase9-router&severity=critical'
curl -s 'http://127.0.0.1:8000/api/v1/stats/alerts'
```

Allow Spark batch completion and Elasticsearch refresh before inspecting. Expect
3 snapshots, only v1→v2 and v2→v3 diffs, and logging-disabled/telnet-enabled alerts.
No v1→v3 diff. OpenAPI is at /docs and /openapi.json; CONFIGSTREAM_API_DOCS=false
also disables /redoc. No jq is necessary.

Stop Spark before rebuilding; keep the API running. Delete **only your four known
test-owned physical indices**, never lake history. Initialize matching aliases,
then run the existing committed reader twice:

```sh
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo make search-init
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo CONFIGSTREAM_LAKE_PREFIX=phase9-demo make search-reindex
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase9-demo CONFIGSTREAM_LAKE_PREFIX=phase9-demo make search-reindex
```

The same logical HTTP records return after refresh without restarting the API.
Neither reindex nor the API should add authoritative information absent from lake
history. Stop API with Ctrl-C, then `make search-down` preserves service volumes.

## Tests and next boundary

```sh
make check
make spark-test
make integration-test
make spark-integration-test
make lake-integration-test
make search-integration-test
make api-test
make api-integration-test
```

The API integration marker includes an Elasticsearch-only test and a full
Kafka/Spark/lake test; for just the smallest backend test run
`.venv/bin/python -m pytest tests/api_integration/test_http_search.py`.
Unit tests use an injected fake client, covering endpoints, models, failures,
pagination, UTC and security. Real tests create/delete only owned indices, verify
identical-timestamp pagination, out-of-order pairs, same-checkpoint restart and
two committed-lake rebuilds while the API remains alive. CI adds only the pure
API tests to its quality job; full services remain explicit local tests.

Phase 10 should use committed lake history for reproducible batch analytics and
controlled experiments, separate from these bounded Elasticsearch serving queries.
No dashboard, auth system, external cache, deployment or final benchmark is added.

## Recorded acceptance: 2026-10-11

macOS arm64, Python 3.12.14, Java 21, Spark 4.0.2 and the existing single-node
Kafka/MinIO/Elasticsearch stack. These are correctness checks, not benchmarks.

| Command | Result |
| --- | --- |
| make api-setup | Locked full-demo extras installed |
| make check | Ruff lint/format and 648 unit tests passed (637 existing + 11 search pagination tests) |
| make api-test | 80 service-free API tests passed |
| make spark-test | All 50 existing Spark tests passed |
| make integration-test | All 3 Kafka tests passed |
| make spark-integration-test | All 3 Spark/Kafka tests passed |
| make lake-integration-test | Existing lake test passed |
| make search-integration-test | Both existing search tests passed, including sink failure/restart/rebuild |
| make api-integration-test | Both real API/search and full pipeline/restart/rebuild tests passed |

Total: **789 passing tests** across the explicit suites. TestClient emits one
upstream HTTPX deprecation warning; it is documented rather than suppressed.

Actual additional live commands were `make search-up`, `make kafka-topics`,
`make lake-init`, `.venv/bin/python -m api.main`, and
`.venv/bin/python -m spark.streaming.main` with lake/search enabled, local[2],
latest offsets and fresh checkpoint/lake/index prefix
**phase9-live-accepted-20261011**. `make phase6-demo` used
`PHASE6_ARGS='--order out-of-order --asset-id phase9-router --start-time 2026-10-11T00:00:00Z'`.

The six curl requests documented above (including unfiltered asset alerts and
stats/alerts) all returned 200: latest version 3, two adjacent diffs, and one high
logging-disabled plus one critical telnet-enabled alert. Committed manifests
confirmed 3 raw snapshots, 3 normalized snapshots, 2 diffs and 2 alerts.

After stopping Spark cleanly, `docker compose --profile search stop elasticsearch`
produced healthz=200, readyz=503 and list=503. `docker compose --profile search up
-d --wait --wait-timeout 180 elasticsearch` recovered readiness and identical
records with the **same API process**. Only the four demo-owned indices were
deleted using the explicit index API; no lake objects were deleted. `make search-init`
then two `make search-reindex SEARCH_ARGS='--query-id
8670528f-64de-41a5-bd1a-7c7ee844d6c5'` runs restored the same 3/2/2/0 logical
documents and HTTP payloads, still in that API process. API logs/responses were
checked for forbidden config/secret/internal markers. Local test logs, helper
scripts, checkpoints and service state are ignored; no generated data is committed.

The Elasticsearch-only test paginated five documents (four identical timestamps)
with limit=2 into 2/2/1 with no duplicates/missing IDs. The pipeline test additionally
verified v1/v2 then v3 across a same-checkpoint Spark restart, duplicate snapshot
replay, per-asset independence, and two rebuilds while TestClient remained alive.
