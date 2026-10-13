# Phase 18 — actual services and recovery limits

Existing project-local Lima/Docker and cached Kafka/MinIO images were reused.
The VM remains **two CPUs, 6 GiB RAM, 64 GiB disk**; host is an Apple M3 Pro,
18 GiB physical RAM. Spark runs on the host, `local[2]`, 2 GiB driver heap,
Spark 4.0.2/Java 21, with cached Kafka/S3A connectors. Kafka 4.1.2 and existing
MinIO retain their previous limits. No installation of system infrastructure,
image pull, disk/RAM increase, volume deletion or paid service occurred.

## Tested failure model

Five opt-in owned-fixture tests passed on actual Kafka, Spark checkpoints and
MinIO, rerun on `314897f4ec79d34a93780096b6294fb9b19958cc` in 75.91 seconds.
Four interrupt callbacks before output, after partial output, before
commit, and after commit before checkpoint acknowledgment. The same checkpoint
is restarted after removing the fault. Before restart committed readers see
zero rows for the first three cases, two valid events for the fourth.

Each fixture has two equal-looking UPDATE records at offsets 0 and 63, then a
malformed body at offset 126. Four native prefix occurrences remain four logical
IDs and two fingerprints. An extra source-coordinate delivery plus a complete
archive replay make **nine physical committed rows**, but **four logical events**.
Publisher progress resumption acknowledges zero new events; fresh replay
acknowledges four. Conflicting same-ID attribute content delivered through the
broker fails after earlier commits, without changing the four-event logical view.

Actual MinIO conditional metadata retry is tested: identical metadata is accepted;
changed count is rejected by server-conditional publication and readback. Partial
uncommitted Parquet remains invisible. The fifth test publishes invalid transport:
Spark fails closed, records an error category in the isolated RIS metadata tree,
and publishes no event commit. Source parser exclusions remain a separate private
source-coordinate ledger, not accepted native events.

This proves **logical recovery under single-writer controlled callback failures**.
It does not test power loss, broker failover, retention deletion, concurrent
publishers, all manifest-corruption cases or fully independent attribute decoding.
Mocked/local tests are not substituted for these service results. Full-hour
service recovery and full independent occurrence acceptance remain open.

## Logical versus physical replay

Same query/batch/digest retries reuse committed output. Source retries in later
batches may append physical copies; committed readers verify each checksum,
reject conflicts and deduplicate by source occurrence identity. They do not
deduplicate fingerprints. This cost is explicit, not a global exactly-once claim.
An event log's partial committed coverage is not a complete routing state. Full
selected-archive acceptance additionally requires exact complete oracle agreement.

Local Spark tests cover older-coordinate conflict, partial output, physical
corruption, source provenance and the selected queries. Existing configuration
and RPKI regression suites run unchanged. No new full legacy-bucket or power-loss
test is claimed. See the [physical amendment](phase18-contract-amendment.md).

## Reproduction (explicit opt-in)

Use the already configured private environment and cached images only:

```sh
source .cache/local-kafka-env.sh
limactl start k
set -a; source .cache/minio.env; set +a
docker compose --profile lake up -d --no-build --pull never kafka minio
RUN_PHASE18_SERVICES=1 .venv/bin/python -m pytest tests/phase18_integration -q
```

Credentials, service logs, source/occurrence ledgers and checkpoints stay private.
Stop only the experiment's services after runs; preserve all volumes, old RPKI
data and new isolated test prefixes. Normal CI does not start these services or
download RIS archives. The fixture tests' per-query waits are bounded; real runs
preregister a separate total cap and host/guest guards.
