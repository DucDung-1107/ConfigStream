# Phase 17.1 — bounded real-service integration

This closure reuses Phase 17's importer, independent SQLite oracle, typed Kafka
transport, dedicated Spark processor and conditional MinIO store. It does not
implement another pipeline or start Phase 18. Final executed-stage status is in
[acceptance](phase17-closure-acceptance.md) and the
[daily validation ledger](../workloads/results/phase17-closure/daily-pipeline-validation.json).

## Environment and isolation

The initial PATH/port probe reproduced the earlier unavailable-service state.
Deeper inspection recovered the existing project-local Docker/Lima installation.
The owner approved necessary disk extension: the stopped VM was enlarged from
24 to 64 GiB, retaining two CPUs and 6 GiB RAM. Only cached Kafka and MinIO
images were started. No installation, pull, volume deletion, RAM increase or paid
service occurred. See the [controlled amendment](phase17-closure-amendment.md)
and [environment](../workloads/results/phase17-closure/service-environment.json).

Spark runs on the host with `local[2]`, a 2 GiB driver heap, eight shuffle
partitions and the existing cached Kafka/S3A connectors. Kafka and MinIO use
1.5 GiB and 0.75 GiB container limits respectively inside the 6 GiB VM. This is
a single-machine, single-writer experiment, not a distributed cluster benchmark.

Each stage has a fresh query UUID, topic, checkpoint and MinIO prefix. The
already allowed `temporal.rpki.fixture.<UUID>` topic namespace is used for both
owned smoke fixtures and pinned real data; the name does **not** classify the
evidence. Keys are observation IDs, transport is `rpki-source-1.0`, and physical
metadata remains `rpki-1.0`. Prefixes are `phase17-fixtures/<UUID>` for owned
fault tests and `phase17-closure/<UUID>` for real stages. Existing configuration
topics, paths, manifests and checkpoints are unchanged. No broker reset occurs.

After final readbacks and regressions, only these two services and the VM were
stopped again. The approved disk remains 64 GiB. Volumes, topics, object prefixes,
source/oracle caches and C/D/E research checkpoints were retained; no experiment
process remains running. Owned fault-test checkpoints use pytest-managed scratch
directories; persistent fault summaries and MinIO/Kafka fixture evidence identify
the tested runs.

## Source, completion and recovery boundaries

All fourteen cached XZ artifacts are checked against the original pinned SHA-256
and byte sizes. No source download occurs. The independent oracle and original
Phase 17 results remain untouched. Per-day source-map and canonical-set exact
anti-joins guard completion; bounded counts merely trigger those checks.

Daily ingestion publishes bounded chunks, waits for broker acknowledgments,
then drains an `availableNow` Kafka query. Subsequent days use the same isolated
checkpoint. At most 256 chunks enter a callback. A partial micro-batch commit
does not imply a complete daily observation. Complete descriptors are separately
published only after full physical-ledger and independent oracle validation.

Eight owned-fixture failure points use the **actual broker, Spark checkpoint
and S3 object store**: before raw output, partial raw output, before raw commit,
after raw commit/before checkpoint acknowledgment, before daily completion,
after member output/before completion, after daily completion/before boundary,
and after boundary/before checkpoint acknowledgment. The callback exception is
removed and the same checkpoint restarted. Two duplicate broker chunks remain
seven distinct raw source rows, two states of three members, and one addition
plus one removal. An additional invalid-transport test fails closed, records a
quarantine entry and publishes no raw/state history.

These are controlled callback failures, not OS power loss, broker failover or
multiwriter races. No cross-system atomic/exactly-once transaction is claimed.
MinIO conditional publication is tested against the server: identical metadata
retry succeeds, conflicting content raises an error, and uncommitted partial
files are invisible to committed readers. Test details and results are in
[checkpoint recovery](../workloads/results/phase17-closure/checkpoint-recovery-validation.json)
and [MinIO validation](../workloads/results/phase17-closure/minio-publication-validation.json).

The existing Phase 12 configuration manifest is also read through its unchanged
committed reader. This bounded check covers six snapshots in one existing batch,
not every historical configuration object.

## Measurement and safety

The first one-day stage completed in 298.913 seconds; the two-day stage completed
in 595.527 seconds with 720,732 exact logical source rows, 717 additions and 437
removals. Full-window results must be read from the final ledger, not extrapolated.
The owner approved a 7,200-second full-window cap **before its first attempt**;
per-day drain remains capped at 1,800 seconds. Earlier stages retain 3,600 seconds.

Host process-tree RSS is sampled every two seconds; guest free disk/memory every
ten seconds. Guards retain 20 GiB free on both filesystems, 15 GiB aggregate
host/guest per-run growth, 50 GiB host experiment outputs, 12 GiB sampled Spark
tree RSS and 512 MiB available guest RAM. Monitoring filesystem/subprocess errors
fail closed. Sampling does not prove instantaneous peak usage or capture every
container transient. Kafka serialized bytes mean acknowledged JSON **value**
bytes, not wire traffic. Spark progress `numInputRows` counts Kafka chunks and
may count repeated Spark scans; accepted source rows come from the logical ledger.

Raw archives, source identities, normalized rows, checkpoints, service logs and
credentials remain private. Only code, owned fixtures and aggregate/digest
summaries are published; see [rights boundary](phase17-closure-correctness.md#audit-and-rights).

## Reproduction (explicit local opt-in)

Use the already configured private environment, verified cache and independent
oracle. Do not source an untrusted script or print credentials. These commands
are not normal CI and must not be used to install/reconfigure infrastructure:

```sh
source .cache/local-kafka-env.sh
set -a; source .cache/minio.env; set +a
RUN_PHASE17_SERVICES=1 .venv/bin/python -m pytest tests/phase17_integration -q
.venv/bin/python -m benchmarks.rpki_service --days 1 --legacy-prefix phase12-terrads-serving --output .cache/phase17-closure/new-day1
.venv/bin/python -m benchmarks.rpki_service --days 2 --legacy-prefix phase12-terrads-serving --output .cache/phase17-closure/new-day2
.venv/bin/python -m benchmarks.rpki_service --days 14 --total-seconds 7200 --legacy-prefix phase12-terrads-serving --output .cache/phase17-closure/new-day14
```

Use fresh directories; advance only after the preceding stage passes. Do not
delete or reset earlier evidence, retry an oracle failure blindly, or enlarge
timeouts after a failed run. Stop only this experiment's services after completion;
preserve all volumes, topics, prefixes and checkpoints for audit.
