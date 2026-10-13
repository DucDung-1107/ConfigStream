# Phase 18.1 — bounded closure experiments

## Preregistered envelope

Owner approval explicitly allows **three cached archives / 15 source minutes,
1,800 seconds maximum**. It does not authorize a whole-hour service run. The
[execution policy](../workloads/results/phase18-closure/execution-policy.json)
keeps host/guest free disk at least 20 GiB, growth at most 15 GiB/run, experiment
outputs at most 50 GiB, guest available RAM at least 512 MiB and unchanged
two-CPU/6-GiB/64-GiB Lima allocation. Host Spark remains `local[2]`, 2-GiB heap.
The original 1,800-second five-minute pilot cap is not increased after a failure.

The first three compressed inputs total **20,396,504 bytes**. Expected service
growth is approximately 1–2 GiB, and runtime 15–25 minutes: both are planning
estimates, not measured results. A full-hour cap must follow the measured pilot
and separate owner approval. No source download, VM expansion, volume deletion,
paid service or concurrent heavy run is permitted.

## Sequence and truthfulness

| Stage | Required evidence | Results authority |
|---|---|---|
| A | Owned independent attribute, sidecar, corruption and replay tests | Validation and recovery JSON |
| B | Exact first-archive level-3 revalidation and committed source accounting | Selected service result; original runtime is never relabeled |
| C | First three complete archives, exact level-4 projection and 15-minute metadata | Three-archive service result |
| D | Twelve-file independent attributes, exact occurrence/coordinate/fingerprint ledger | Full-hour oracle JSON |
| E | Authorized twelve-file Kafka/Spark/MinIO completion and exact readback | Separate full-hour service result, otherwise NOT_RUN |

Offline source verification can precede expanded service execution. The oracle is
paused while bounded Spark/service fixture checks execute; its original wall-clock
deadline remains unchanged. All partial private outputs and failed attempts remain
available. The accepted prior framing evidence is reused for comparison rather
than rewritten. New attribute verification necessarily rereads the pinned source.

Each real run uses fresh topic, checkpoint, query ID and MinIO prefix. The driver
records code SHA/module fingerprints, source SHA-256s, cap and resource policy
before execution. Source exclusions, artifact descriptors and complete-window
metadata are finalized only after exact physical readback and oracle comparison.

## Service prerequisite failure

The first owned-fixture invocation failed 13 tests at authentication prerequisites:
the reused MinIO container was started without sourcing its existing private root
environment. The correct private environment was restored before retry; no volume
was reset and no credentials are published. Failure logs remain private and this
attempt is not counted as service acceptance. The successful rerun and separate
cross-archive recovery test are reported independently.

The first new five-minute service attempt stopped at final offset reporting:
Spark 4 exposes a Python representation through an offset accessor, which is not
necessarily JSON. The driver now reads Spark's canonical progress JSON and validates
its topic/partition binding. That failed attempt (256.427 seconds) remains separate
from the fresh successful run; no output or old measurement was relabeled.

The accepted revalidation used commit `9cb1b7a`, baseline-full oracle mode and a
fresh isolated service namespace. It committed 370,141 exact logical events,
five source exclusions and a validated complete-window descriptor in 293.494
seconds, with zero broker lag. Kafka acknowledged 2,892 chunks / 269,418,671 value
bytes. Parquet occupies 70,024,542 bytes; sampled process-tree peak is
2,411,872,256 bytes. These are new measurements, not the original Phase 18 run.

## Processing investigation

The approved fifteen-minute attempt drained all 1,303,847 occurrences through
42 committed batches. Exact complete projection comparison and all three
source-order checks completed; 14 exclusions were committed and read back.
It then failed at the final whole-window digest after **1,007.260 seconds**:
`Java heap space` in Spark's task-result-getter, followed by context shutdown.
No complete-window descriptor was published. This is **not** fifteen-minute
window acceptance, despite final broker lag zero. Private failed output is retained.

Read-only post-failure inventory finds 168 Parquet files / 247,864,779 bytes,
42 batch commits declaring 1,303,847 physical rows and 9,190 sidecar bytes.
These metadata counts do not substitute for a renewed successful complete read.
Sampled RSS peaked at 2,873,704,448 bytes; guest disk stayed above
42,910,162,944 bytes free and available guest RAM above 4,529,483,776 bytes.
The failure is within the Java 2-GiB heap, not host/guest disk exhaustion or a
breach of the sampled 12-GiB process-tree guard.

Spark was configured with **two shuffle partitions and AQE disabled**. The
ordered digest's `toLocalIterator` therefore fetched a large sorted partition;
the trace places heap allocation failure in task-result retrieval. Spark documents
that [local iteration consumes memory proportional to the largest partition](https://spark.apache.org/docs/4.0.2/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html).
This explains the specific observed failure; no quadratic complexity claim follows.

The correction uses explicit range repartitioning at a target 32,768 rows,
within-partition ordering and a distributed maximum-partition-count barrier before
iteration (maximum 131,072 rows, 4,096 partitions, no prefetch). Ordered JSON/SHA
bytes are unchanged. Skew fails closed instead of risking an unbounded result.
Owned independent order vectors and a deliberate skew fixture test the rule.
It is a capacity correction, **not a measured speedup**, and does not authorize a
new real-data run by itself. The owner subsequently approved one same-cap retry;
it completed in 1,056.353 seconds with all 1,303,847 exact events, complete-window
metadata, 14 exclusions, query agreement and zero lag. The full-hour service cap
still requires separate approval. Detailed [accepted retry evidence](../workloads/results/phase18-closure/three-archive-validation.json)
is distinct from the preserved failed attempt and is not a speedup claim.

The RIS callback does **not** reread prior committed Parquet on every batch. Its
actual repeated work is validation against the selected full oracle, sorting the
incoming logical projection for a digest, and batch publication. Final readback
separately verifies every physical batch before logical reconciliation. These
costs must not be mislabeled as the earlier RPKI cumulative-ledger algorithm.

New callback timers separate verification, digest and publication. They are wall
times, not exact Spark task I/O or shuffle counters. Scope fields identify which
artifacts are eligible for oracle scanning, not a measured number of physical
scans. Spark scheduling/recomputation may perform additional work. Uninstrumented
task bytes, shuffle and spill remain N/A rather than fabricated zeros.

An opt-in `--scoped-oracle` restricts expected rows to a bounded, at-most-twelve
artifact metadata set. Both modes retain exact same-ID conflicts and final full
independent comparison. Owned tests establish projection parity, **not a speedup**.
The default remains the original full-oracle mode until matched performance evidence
justifies a preference. No immutable output format is reinterpreted or rewritten.

## Measurements and fair comparison

Producer ACK occurrences, Kafka chunks, serialized value bytes, physical batch
rows, reconciled logical occurrences, selected broker log bytes, Parquet/metadata/
checkpoint bytes and source-time rates use separate denominators. Final lag compares
Spark source offsets against actual selected broker watermarks. Query validation
checks full projection and interval/peer/prefix/action/minute/fingerprint/provenance
views. A committed window is not a complete routing table.

The bounded driver publishes one complete cached artifact, then drains it with
Spark `availableNow`, repeating for each selected artifact. Producer and drain
timings therefore describe **sequential per-archive historical replay**, not a
continuously paced live BGP feed or overlapping producer/consumer steady state.
No historical-to-current timestamp subtraction is reported as operational latency.

The existing oracle's integrity, independent decoding, SQLite and Parquet time is
not a matched batch speed baseline. Original five-minute service measurements,
new larger windows and warm fixture tests are not interchangeable before/after
trials. No streaming superiority, steady-state throughput or scalability law is
claimed. Matched repetitions and paced replay remain Phase 20 work, not implicitly
authorized by this closure phase.

## Commands

```sh
make check
make api-test
make spark-test
source .cache/local-kafka-env.sh
set -a; source .cache/minio.env; set +a
limactl start k
docker compose --profile lake up -d --no-build --pull never kafka minio
RUN_PHASE18_SERVICES=1 .venv/bin/python -m pytest tests/phase18_integration -q
.venv/bin/python -m benchmarks.ris_service --files 3 \
  --oracle .cache/phase18-closure/oracle-hour-v2 \
  --output .cache/phase18-closure/service-fifteen --max-seconds 1800
```

Normal CI does not start services or download source data. Raw archives, complete
normalized ledgers, checkpoints and service logs remain private. Results identify
the actual commands executed; a reproduction command alone is not a test result.

## Rights and attribution

Source: RIPE NCC / RIPE RIS, twelve pinned `rrc00` archives. The
[RIPE NCC copyright statement](https://www.ripe.net/about-us/legal/copyright-statement/)
addresses research/educational use with identification conditions. The
[RIS commercial terms](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/commercial-use/)
are not blanket permission for this project's raw-data redistribution. Keep
external raw/normalized records private; public artifacts contain only aggregate
statistics, permitted references and digests. Additional redistribution and
instructor approval remain unverified. Do not apply unrelated RIPE Database or
Data Repository terms as though they automatically govern this raw RIS endpoint.

## Reviewer-facing limits

Contribution: auditable selected native-event preservation, not a novel routing
algorithm. Clarity: distinct source, occurrence, batch and window boundaries.
Effect: correctness precedes throughput; no matched speedup claim. Evaluation:
full-hour service acceptance needs separate evidence/approval. Method limits:
pinned exclusion disposition and a declared supported wire subset, not universal
BGP validity, routing state or operational incident causality.
