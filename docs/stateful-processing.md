# Stateful configuration processing

This page describes the current configuration path, originating in Phase 6 and
updated by the [source-identity and publication amendment](pipeline-integrity-amendment.md).
Historical acceptance receipts retain their original operator/checkpoint versions.

## Implemented topology

One Structured Streaming query reads `config.snapshots` directly from Kafka:

```text
Phase 4 parse / SQL validation
  ├─ invalid ───────────────────────────────────────> safe diagnostics
  └─ valid -> event_time watermark -> snapshot_id binding -> groupBy(asset_id)
              -> native version state -> Phase 5 normalize/diff -> YAML rules
                                                          └─> Alert
Combined output -> one foreachBatch boundary -> optional committed lake -> optional search
                                                                          -> config.alerts
```

There is no dependency on `config.events` or `config.assets`, nor an assumption
that Kafka partitions equal Spark state partitions. Spark shuffles by `asset_id`.
Pairing uses **asset identity and version adjacency**, never the previous Kafka
row, global arrival order, or timestamp order. JSONL/generator contracts are unchanged.

## Runtime and native state API

The installed PySpark 4.0.2 API and a real local streaming query were verified
before adopting `GroupedData.transformWithStateInPandas`, `StatefulProcessor`,
`StatefulProcessorHandle`, and event-time timers. No fallback, external state
database, Python Kafka consumer, or process-global state dictionary is used.
See the [Spark 4.0.2 API](https://spark.apache.org/docs/4.0.2/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.transformWithStateInPandas.html)
and [native state guide](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-transform-with-state.html).

Unchanged: Python 3.12, Spark/PySpark 4.0.2, Java 21, Scala 2.13, Kafka 4.1.2,
connector `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`.
The Spark extra adds pinned pandas 2.2.3, PyArrow 19.0.1, and protobuf 5.29.5.
The installed Spark state protocol was generated with protobuf 5.28.3; the pinned
runtime meets that requirement. PyYAML 6.0.2 is a base dependency for service-free
rule loading. All resolved dependencies are in `uv.lock`, not installed at query time.

## State representation and ownership

RocksDB is the Spark-managed state-store provider. This is not a separate service.
Per grouping key, `VersionProcessor` declares:

| Native state | Typed contents |
| --- | --- |
| `versions_v1` ListState | snapshot ID (string), version (long), UTC event-time microseconds (long), normalized content (string), emitted-next-edge flag (boolean) |
| `eviction_floor_v1` ValueState | greatest capacity-evicted version (long) |
| Event-time timer | maximum retained accepted event time + configured timeout, in milliseconds |

Normalized content is the only retained configuration body; raw JSON, raw content,
ingest timestamps, and unrelated Kafka payload copies are not stored. Hashes are
recomputed from retained normalized content when reconstructing Phase 5 objects;
normalization itself is not repeated. Original event-time microseconds are kept
for exact Alert timestamps, while Spark timer deadlines use milliseconds.

Spark checkpoints the buffers, edge flags, floor, timers, source offsets, and
dedup state. The pure `processing/state.py` transition structures are temporary
values loaded from and written back to that native state, not another database.
Each invocation retains at most the configured number of versions plus one
candidate during the transition; it does not collect all input history.

## Version admission and pairing

1. Native `IdentityProcessor` validates the source payload and binds `snapshot_id`
   before asset grouping. Exact retained retries are suppressed; conflicting source
   bindings emit `snapshot_identity_conflict`, including cross-asset ID reuse.
   The pure transition also compares retained content/version/time before treating
   an identity as an exact retry. Ingest time is delivery metadata.
2. An already-retained version with another snapshot ID produces
   `version_conflict`. **First accepted version wins**; the conflicting record
   does not alter state or generate a diff. Within a batch, Spark iteration order
   is not a promise of first Kafka offset. Conflicting source histories are invalid;
   a fresh replay is not guaranteed to select the same winner for such input.
3. Versions at/below the capacity-eviction floor produce `evicted_version`, without
   reconstructing discarded history. These checks precede normalization.
4. A new eligible snapshot is reconstructed using the existing ConfigSnapshot,
   including its inline hash validation, then normalized using Phase 5 exactly once.
   URI-only content, unsupported formats, unsafe multiline secret syntax, or stricter
   model failures become `processing_rejected` diagnostics, not raw error dumps.
5. Insert into the bounded buffer sorted by version. For every available adjacent
   `vN, vN+1` whose edge flag is not set, call Phase 5 `compare_snapshots`, evaluate
   rules, and set the flag. Keep both endpoints for later neighboring versions.

Examples: `1,3,2` emits only `1→2` and `2→3`. `2,3,1` emits `2→3`, then `1→2`.
A first observation of v7 emits no baseline; v6 arriving later can complete v6→v7.
A zero-change pair still emits a valid zero-count ConfigDiff, but default rules
emit no alerts for secret-only or generated-comment-only changes.

## Watermark, dedup, and retention

Configuration is centralized in `SparkConfig` and `.env.example`:

| Setting | Default | Meaning |
| --- | --- | --- |
| `SPARK_WATERMARK_DELAY` | `10 minutes` | Delay on snapshot `event_time` |
| `SPARK_STATE_TIMEOUT` | `30 minutes` | Per-asset inactivity in event time |
| `SPARK_MAX_VERSIONS_PER_ASSET` | `32` | Maximum retained versions per asset, minimum 2 |
| `CONFIGSTREAM_RULES_FILE` | `rules/default.yaml` | Validated once at application startup |
| `SPARK_CHECKPOINT_DIR` | `.cache/spark-checkpoints/config-snapshots-v9` | Policy-bound source-identity-v2 checkpoint |

Durations accept a positive integer followed by milliseconds, seconds, minutes, or
hours (singular/plural). Invalid durations/caps and invalid/missing rules fail startup.
Shell values containing spaces must be quoted; `.env.example` is not a shell script.

`withWatermark("event_time", delay)` precedes native source-identity binding via
`transformWithStateInPandas`. This replaces the original Phase 6
`dropDuplicatesWithinWatermark` topology. IDs must be globally unique and immutable;
conflicts are detected only while their binding remains retained. This is not an
eternal uniqueness index. Late records can be dropped once the operator watermark
advances. A checkpoint marker binds the state contract, shuffle partition count,
watermark delay, timeout, version cap, loaded rule content, Kafka source/alert
cluster, lake destination and search destination. Restarting with a different
policy or sink is rejected before query startup, so acknowledged offsets cannot
silently skip a newly enabled destination. Credentials and throughput settings
may change without changing the contract. Rules are hashed; credentials and rule
contents are not stored in the marker.

The v9 default adds these policy bindings without changing the stateful operator
layout. Old v8 markers cannot prove which policy produced their state and are
rejected, as are unmarked checkpoints. Use an isolated v9 checkpoint with explicit
starting offsets and a scoped replay/backfill plan. Existing checkpoints and
accepted evidence must remain intact. Reusing the same bootstrap address after
replacing the underlying Kafka cluster is outside this local marker's guarantees.

The watermark follows observed event-time progress and micro-batch/operator
propagation, not elapsed wall-clock time or Kafka timestamps. It does **not** mean
every record older than exactly ten wall-clock minutes is immediately discarded.
Records from a quiet asset can be late relative to other assets. Future-dated
source events can advance the watermark aggressively; source clock quality matters.

The separate 30-minute timeout is an **asset inactivity** policy: deadline = largest
retained accepted event time + timeout. Register one event-time timer per asset,
replacing its prior timer when updating state. When the watermark reaches the
deadline and Spark schedules processing, clear all buffer/floor/edge state and
emit `state_timeout` plus any known unresolved gaps. Input handling also checks
expiry before admission, so an expired buffer is not reused for a new arrival.
No processing-time TTL is substituted for this event-time policy.

Expiry removes the most recent baseline as well as the reorder buffer. If an asset
changes again after its state has expired, its first new snapshot cannot generate
a diff against the forgotten baseline. This matters for sparse configuration
histories: the current streaming path does not guarantee every adjacent pair over
arbitrary inactivity gaps. Historical analytics counts committed diffs and does
not reconstruct these missing pairs. A durable baseline or explicit batch repair
path is needed before claiming complete sparse-history change detection.

An active asset can retain older versions until the size cap removes them; this
is not individual-snapshot TTL. If event time stalls, timers/dedup cleanup can
stall even while wall time passes. Total state also depends on active asset count,
snapshot sizes, and input rate; a 32-version cap is not a global byte/memory limit.

## Capacity and gaps

When an insertion exceeds the cap, first emit any newly available adjacent pairs,
then evict the **lowest version**, emit `state_eviction`, and advance the floor.
Discard edge flags whose endpoints are no longer retained. At most cap−1 flags
remain. The floor blocks old replay from recreating already-forgotten edges.
This deliberately sacrifices very old late neighbors under capacity pressure.

Never compare v1 directly to v3 in the streaming path. Keep the gap while retained.
On capacity eviction of a gap endpoint or asset timeout, emit
`unresolved_version_gap` with bounded known versions and a missing start/end range.
The missing count is `missing_end - missing_start + 1`; missing versions are not
expanded into an unbounded array. Unknown history before the first observed version
is not reported as a gap. A lost endpoint cannot subsequently be repaired here.

Diagnostics contain IDs, versions/ranges, event/watermark timing, and fixed reasons,
never raw or normalized configuration. Phase 4 invalid diagnostics retain Kafka
key/topic/partition/offset/timestamp via the existing safe projection. DLQ production
is still deliberately absent.

## YAML rules and Alert contract

```yaml
rules:
  - id: logging-disabled
    enabled: true
    severity: high
    operation: added
    pattern: "^logging disabled$"
    message: "Configuration logging was disabled"
```

The loader uses safe YAML parsing and Pydantic validation before Spark starts.
Duplicate IDs, unknown fields/severities/operations, blank/invalid regex patterns,
missing messages, and malformed files fail startup. `enabled` defaults to true.
Each enabled rule uses `re.fullmatch` against one normalized DiffLine whose operation
matches `added`, `removed`, or `any`. A rule emits at most one Alert per ConfigDiff,
even when several lines match. Several rules may fire on the same diff.

Defaults: logging-disabled (high), telnet-enabled (critical), debug-enabled
(medium), nginx-access-log-disabled (high). Rules do not inspect source secrets.
Regex/rule files are trusted operator configuration, not an untrusted user DSL;
catastrophic regex cost is not sandboxed. Version rule IDs when changing meaning.

`evaluate_rules(diff, event_time, rules)` reuses the existing Alert/Severity models.
Snapshot ID is `diff.new_snapshot_id`. Alert ID is UUID5 over canonical JSON
`["configstream:alert:v1", diff_id, rule_id]`. `created_at` equals the new snapshot's
event time, including microseconds, **not** processing time. Fixed input and rules
reproduce the entire payload across processes. Phase 5 Diff IDs remain unchanged.

## Output boundary, observability, and delivery

The stateful operator emits typed rows: record type, asset ID, event time, a safe
summary JSON, and an optional value. Values hold ConfigDiff JSON or the shared
Alert envelope; summaries contain only operational metadata. Invalid diagnostics
are unioned into the output of the **same query**, without a second stateful run.

One `foreachBatch` persists the output for full consumption, bounded-cardinality
counts, a capped summary sample, and a distributed Spark Kafka batch write. It
always unpersists, including on failure. It never collects whole production batches
or uses a driver-side producer per output row. Diff values/config content are not
logged or durably stored in this phase. Accepted-snapshot counts are post-dedup and
post-processing-validation, not raw input counts.

Alerts use topic `config.alerts`, UTF-8 `asset_id` keys, and existing
`{"record_type":"alert","payload":{...}}` serialization with payload schema_version.
Spark's Kafka writer uses `acks=all`, producer idempotence, 10-second delivery and
5-second request deadlines. Topic settings remain 3 partitions, RF=1, delete/7-day
retention; automatic creation is not relied upon. Generator publishing is unchanged.

Native progress logs retain batch ID, input/processed rows per second, input count,
watermark, and state-operator metrics (including dedup drops and watermark drops
when exposed). Output logs count accepted snapshots, diffs, alerts, and diagnostics
by reason. Samples are limited by `SPARK_CONSOLE_ROWS`. Union branches and retries
can affect native input/rate metrics; do not treat them as a unique logical event
counter or a benchmark. No Prometheus or business window analytics is added.

Spark checkpointed state supports restart/recovery. **No end-to-end exactly-once
claim:** a sink write can succeed before a batch commit fails, so retries/replays
can append duplicate physical alerts. Deterministic IDs enable future serving
storage to upsert by alert_id; the Kafka log itself does not enforce that uniqueness.
See [Spark Kafka sink guarantees](https://spark.apache.org/docs/4.0.2/streaming/structured-streaming-kafka-integration.html).

## Restart and failure policy

| Situation | Behavior |
| --- | --- |
| Restart with same compatible checkpoint | Restore offsets, dedup, buffers, timers, emitted edges; completed pairs are not recalculated from source replay alone |
| Change startingOffsets with same checkpoint | Saved offsets win; this is not a history replay request |
| Delete/change checkpoint | State/offset memory lost; retained Kafka history may produce the same logical IDs again |
| Duplicate replay | Bounded native dedup plus retained identity/edge guards; not indefinite global dedup |
| Version conflict | First accepted retained version wins, diagnostic emitted |
| State timeout | Drop all asset state; later fresh observations have no old baseline |
| Buffer cap | Evict smallest version, advance floor, report eviction/gaps |
| Kafka alert write fails | Fail the micro-batch/query visibly; do not silently mark alerts delivered |

Never share checkpoints between queries, `--parse-only`, and Phase 6. The new v6
default deliberately avoids the Phase 4 console checkpoint. Keep state schemas,
partition count, normalization policy, rules, and retention settings fixed for a
recovery test. Incompatible changes require a planned migration/new checkpoint;
this project does not automatically migrate or delete checkpoints.

## Commands and acceptance

```sh
make setup
make check
make spark-setup
make spark-test
make kafka-up
make kafka-topics
```

Terminal A, choose an unused path for first-run acceptance:

```sh
SPARK_MASTER='local[2]' SPARK_STARTING_OFFSETS=latest \
SPARK_CHECKPOINT_DIR=.cache/spark-checkpoints/phase6-example \
make spark-stream
```

Wait for the initial `Progress` log. Terminal B:

```sh
make phase6-demo PHASE6_ARGS='--order normal --asset-id demo-normal'
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id demo-ooo'
make kafka-consume-alerts KAFKA_CONSUME_MAX_MESSAGES=4
.venv/bin/python -m scripts.kafka_invalid_demo
```

Each demo emits three valid snapshots; each asset produces exactly v1→v2 and
v2→v3, with logging-disabled and telnet-enabled alerts. Consumer CLI prints keys
and values from retained history, so on a reused broker its first four records
may be older demos. The integration test records starting offsets to avoid this
ambiguity. Stop the live query before integration tests to avoid concurrent writers.

```sh
make integration-test
make spark-integration-test
```

The fixture uses fixed start `2026-10-05T00:00:00Z`; `--start-time` overrides it.
Generator defaults instead use 2026-09-30. Use consistent clocks or fresh checkpoints
when mixing workloads; older fixed-time demos may correctly be dropped as late.
For the ordinary generator with the same clock:

```sh
.venv/bin/python -m generator.main --assets 3 --events 8 --seed 42 --no-sleep \
  --sink kafka --start-time 2026-10-05T00:00:00Z
```

For bounded replay, use `SPARK_STARTING_OFFSETS=earliest`, an unused checkpoint,
and `make spark-stream SPARK_ARGS=--available-now`. To reproduce the Phase 4
diagnostic query, choose another checkpoint and `SPARK_ARGS=--parse-only`.
After testing, stop Spark with Ctrl-C and run `make kafka-down` (preserves data).

Tests cover all permutations of v1/v2/v3, 100 seeded permutations with duplicate
replay, independent assets, version conflicts, cap/floor behavior, gaps, and expiry.
Rules/transition tests require no Spark/Java/Kafka. Service-free native Spark tests
exercise checkpointed state, dedup, out-of-order history, capacity, and timers.
Their controlled clock advances from seconds 1–6 to seconds 100/101 with a 10-second
watermark and 30-second state timeout: old state expires, unresolved gaps are
reported, and a subsequently arriving second-5 record is dropped. They assert
native duplicate/watermark-drop counters instead of sleeping for event-time minutes.

Real Kafka integration launches **separate Python/Spark processes** with the same
checkpoint, verifies exactly adjacent pairs and deterministic IDs, replays records,
tests conflicts/secret rotation/invalid input, and consumes schema-valid keyed
alerts. It uses small development records and never deletes topic data. Normal CI
runs pure and service-free Spark tests; full Kafka/Spark integration stays explicit.

## Recorded local acceptance

On 2026-10-05, macOS arm64, Python 3.12.14, Java 21, Spark 4.0.2, and the
project's Kafka 4.1.2 broker:

| Command | Result |
| --- | --- |
| `make setup` / `make spark-setup` | Locked dependency installation succeeded |
| `make check` | Ruff lint/format passed; 579 unit tests passed |
| `make spark-test` | 44 service-free Spark tests passed |
| `make integration-test` | 3 Kafka integration tests passed |
| `make spark-integration-test` | 3 Spark/Kafka integration tests passed |

The live stream used `SPARK_MASTER='local[2]'`, `SPARK_STARTING_OFFSETS=latest`,
and a fresh `.cache/spark-checkpoints/phase6-live-20261005` checkpoint. Commands
after the initial progress report were:

```sh
make kafka-demo
make phase6-demo PHASE6_ARGS='--order normal --asset-id phase6-live-normal'
make phase6-demo PHASE6_ARGS='--order out-of-order --asset-id phase6-live-ooo'
.venv/bin/python -m scripts.kafka_invalid_demo
make kafka-consume-alerts KAFKA_CONSUME_MAX_MESSAGES=12
```

Observed 14 accepted snapshots, 10 diffs, and 7 alerts from the generator plus
both dedicated histories. The four malformed records produced diagnostics
without stopping the query. Both dedicated histories produced exactly v1→v2
and v2→v3, with two schema-valid, asset-keyed alerts each. The consumer included
five alerts from the preceding integration run plus seven live alerts; assertions
filtered the demo asset IDs rather than assuming an empty topic. No configuration
bodies appeared in operational logs. Ctrl-C stopped the query cleanly.

The integration suite separately proved recovery across fresh Spark processes
using the same checkpoint: replay did not re-emit the old pair, and the next
version emitted only its new adjacent pair. Controlled event-time tests proved
deduplication, late-row dropping, gap/timeout diagnostics, and capacity eviction.
These are correctness smoke tests, not throughput or production-latency benchmarks.

## Remaining limits

Phase 7 now supplies optional [MinIO/Parquet history](data-lake.md), preserving the
Phase 6 state policy and adding accepted normalized snapshots to lake-mode output.
Full DLQ, rule sandbox, global state byte quota, and cluster/throughput claims
remain absent. Supported-secret masking is not universal redaction; protect
checkpoints and cached output as potentially sensitive. Kafka delivery remains
at-least-once even with the added lake commit protocol.
