# Configuration pipeline integrity amendment — 2026-10-15

Decision `2026-10-15-CONFIG-INTEGRITY-V2` addresses the three reproduced defects in
`workloads/results/final-course/pipeline-review-20261015.json`. That negative
receipt and all Phase 15–20 evidence remain unchanged. No report/slides or new
real-source benchmark is part of this repair.

## Physical publication and replay

New configuration `_commits` use format `2.0`, adding a sorted, relative Parquet
file inventory with byte lengths and streaming SHA-256 checksums per dataset.
Readers compare the complete file inventory and physical row counts before
returning data. Missing, extra or changed files fail closed. Hadoop file streams
are hashed incrementally; entire Parquet objects are not collected into Python.
This adds integrity I/O, not a claimed throughput improvement.

Replaying a committed query/batch compares the **full typed projection as a
multiset in both directions** against committed data. Matching counts alone is
not acceptance. An exact retry does not rewrite Parquet or old manifests.

Format `1.0` remains readable and exact candidate replay is checked. Legacy row
counts now fail closed on partial files. Legacy manifests have no recorded file
checksums: same-count historical corruption cannot be retrospectively ruled out
without an independent oracle. No old manifest is silently upgraded.

These are single-writer, immutable-publication guarantees, not distributed ACID.
A whole missing commit cannot be inferred from a listing without an external
expected inventory/completion barrier. Files must remain immutable after reader
validation; this is not protection against concurrent malicious mutation.

## Bounded source identity

A native Spark stateful identity guard precedes asset-grouped version processing.
It validates the source payload before binding a snapshot ID, including asset,
version, source time, format, source content hash and source URI. Ingest time is
delivery metadata and does not change the source binding. Raw content is not
stored in the binding; validated SHA-256 binds it. Equal-looking records with
different IDs are not payload-deduplicated.

Within retained event-time state, an identical retry is suppressed; a different
binding emits `snapshot_identity_conflict` and never creates a new accepted
normalized row. This also catches cross-asset conflicts before asset grouping.
Original raw Kafka deliveries remain archived. The pure bounded version state
also compares normalized content/version/time rather than treating a known ID
alone as proof of an exact retry.

Identity retention is bounded by the configured state timeout and watermark.
Late input remains raw evidence, not accepted timeless history. This does not
establish arbitrary-age global ID uniqueness or end-to-end exactly-once delivery.

The operator/state layout changed. The new default is `config-snapshots-v8` and
new checkpoints contain `configstream-state-contract.json`, binding contract
version and shuffle partition count. Unmarked existing checkpoints and partition
changes are rejected before starting a query. Do not delete/reset old state or
resume an old query under the new layout. Use a fresh isolated prefix/checkpoint
and explicitly selected offsets for rollout; keep the old image for old jobs.

## Targeted cost reductions

- RIS summaries reuse their occurrence count; scoped completed-window readers
  retain checked materialization through caller actions. The compatibility reader
  remains available but is not recommended for repeated full-window actions.
- DNS finalization asks the artifact-scoped committed reader for complete exact
  reconciliation once, instead of repeating partial and full reconciliation.
- Search skips known-empty record types using the already computed batch counts.
- RPKI/RIS/DNS producers support `inflight_chunks=1..32`; default `1` retains the
  earlier experiment policy. Each window is bounded to 8 MiB serialized payload.
  Progress advances only after **every** callback in that window succeeds, the
  flush drains and the resource check passes. Out-of-order ACKs cannot skip a hole.
  A failed window is poisoned. A crash before progress replacement may replay the
  window; source identities, not producer idempotence alone, reconcile retries.
- Batch analytics may use a separate AQE profile (eight initial partitions by
  default). Streaming retains fixed partitions (two by default) and AQE off.
  `SPARK_BATCH_SHUFFLE_PARTITIONS` and `SPARK_SHUFFLE_PARTITIONS` are independently
  bounded controls. Do not change streaming partitions on an existing checkpoint.

No speedup is inferred from fewer calls without matched measurements. Existing
Phase 20 measurements remain bound to their original execution commits/settings.

## Cached runtime build (2026-10-16)

`infra/k8s/scripts/build.py --cached-runtime --campaign-directory <live-campaign>`
uses `Dockerfile.cached` and the digest-pinned, locally retained accepted ARM64
application runtime. It rejects changes to `uv.lock`, `pyproject.toml` or the full
runtime Dockerfile relative to that runtime's accepted source commit. The image
also compares dependency inputs against the actual base-image files before
replacing application code. Old application directories are removed first;
untracked local files are excluded by `git archive`.

This avoids resolving unchanged Maven/Python dependencies on every source-only
repair. It is not a clean-machine rebuild result. The default full Dockerfile
remains the clean-machine path. The preceding full-build failures (host swap
guard and AWS SDK Maven download) remain preserved in private campaign receipts.

## Course scope

The [official course page](https://datalab.vn/teaching/it4043e-big-data-storage-and-processing)
requires an end-to-end Lambda/Kappa system, Spark, messaging, NoSQL, equivalent
distributed storage and Kubernetes/cloud. Existing owned-fixture Kubernetes and
dashboard evidence establishes the executed local teaching stack, not multi-node
HA or replicated local-path storage. Whether this local S3-compatible profile
satisfies the instructor's storage expectation still needs confirmation.

The owned Spark competency suite demonstrates advanced transformations, joins,
aggregation, pruning, persistence, state and plans. MLlib/GraphFrames remain
unimplemented, not silently passed; their course applicability needs instructor
confirmation. The page is labeled 2020, and current-semester administrative
requirements remain unverified. H3 paced RIS replay remains NOT_RUN. The owner
cancelled report/slide work; this repair does not claim complete course submission.
