# Phase 18 — native RIS event history

This is an additive archive-history workload, not RIB reconstruction, incident
detection, ROV, configuration diffs or an API. The central problem and three RQs
in [Phase 15](phase15-research-freeze.md) remain unchanged. Read
[acceptance](phase18-acceptance.md) before treating any window as validated.

## Boundaries

```text
Pinned private gzip archives -> independent MRT coordinates
  -> mrtparse 2.2.0 / explicit unicast expansion
       -> private framed-record and exclusion ledgers
       -> bounded RIS Kafka chunks -> dedicated Spark foreachBatch
            -> exact declared-source projection validation
            -> Parquet files -> conditional immutable commit
                 -> committed-only logical event history -> bounded queries
Independent binary NLRI/identity oracle -----------^ acceptance checks
```

| Responsibility | Module | Boundary |
|---|---|---|
| Typed transport/projection | `schemas/ris.py` | Reuses Phase 16 `Evidence`, `MrtCoordinate`, `RisOccurrence`, provenance and identity |
| Framing/body expansion | `processing/mrt_framing.py`, `processing/ris.py` | Framer unchanged; body failures never change later offsets |
| Independent reference extraction | `workloads/ris_reference.py` | Separate binary cursor/NLRI decoder and identity implementation; no production expander imports |
| Oracle/batch projection | `workloads/ris_oracle.py` | Verified cache only; private Parquet and disk-backed fingerprint inventory |
| Full framing audit | `workloads/ris_framing_audit.py` | Independent complete-file framing, CRC and excluded-coordinate accounting; not full occurrence acceptance |
| Broker | `messaging/ris.py` | Bounded ACK progress, dedicated topic, restart from accepted-event position |
| Spark | `spark/ris.py` | Finite pinned-source validation, immutable batches, logical identity reconciliation |
| Lake | `lake/ris.py` | Separate `_ris_commits/v1`; registered `temporal/ris/occurrences/physical-v1` |
| Queries | `analytics/ris.py` | Time/prefix, peer activity, actions, minute bursts, fingerprints, provenance |
| Opt-in execution | `benchmarks/ris_service.py` | Fresh isolated run, explicit runtime cap and fail-closed resource guards |

## Physical contract

Source gzip is retained once in private cache, referenced by compressed SHA-256
and safe URL. Full coordinates and excluded-frame ledgers are private; only safe
metadata/digests are public. Event Parquet contains source identity, both MRT
coordinates, expansion ordinal, recorded time/precision, peer ASN/IP, action,
AFI/SAFI, prefix, optional path ID, attribute digest and descriptive fingerprint.
The manifest reference resolves parser, canonicalization and counting policies.
Attributes are recoverable from the pinned archive and parser, not retained as a
second raw archive copy or advertised as independently decoded AS paths.

Partition granularity is isolated query UUID / bounded micro-batch, four Parquet
writer partitions. Collector/time/artifact remain columns. This deliberately
avoids per-prefix/per-occurrence files; time-layout/compaction comparisons remain
Phase 20 work. No checkpoint or configuration/RPKI layout changes.

Commit `ris-1.1` binds sorted projection checksum, logical batch count, manifest
IDs and artifact hashes; pinned references have separate conditional metadata.
See [P18-M1](phase18-contract-amendment.md) for experimental `ris-1.0` handling.
Readers enumerate commits, validate their exact registered paths/checksums and
provenance, then reconcile equal logical identities. They never discover history
by scanning output directories. Missing/corrupt files or conflicting IDs fail.

## Processing and recovery model

Every incoming distinct projection must match the declared oracle exactly. A
same-ID changed payload fails; different IDs with equal fingerprints survive.
Parquet may retain equal redeliveries in *different* committed batches. The
logical reader suppresses these exact source retries after conflict validation;
physical overhead is reported, not called new native events. Batch retries with
the same query/batch digest reuse existing commits without rewriting valid data.

This is a **single-writer, closed pinned-source experiment**. The oracle lookup
is not an online global BGP identity index. Each callback may scan the selected
oracle projection; committed readback verifies every selected batch. No claim
of constant-cost arbitrary archives, multiwriter ACID or Kafka/S3 global
exactly-once transactions follows. Source completeness means archive accounting,
not complete routing state. Incrementally committed valid events may be queried,
but a complete-window claim additionally requires exact full oracle agreement.

The topic is `temporal.ris.source.v1.<UUID>`, two partitions, replication factor
one, seven-day retention. Key = compressed artifact SHA: one source record's
expanded prefixes share a partition; no total order across archives/partitions.
Producer buffers are 16 MiB, JSON chunks at most 256 rows/1 MiB; default 128.
Spark uses an isolated checkpoint, `availableNow`, maximum 256 Kafka chunks per
callback and `failOnDataLoss=true`. This is unpaced retrospective ingestion, not
a demonstration of a continuously running live collector.

Existing configuration serializers/state buffers, snapshots/diffs, alerts,
RPKI states/transitions, analytics and API/search remain unchanged. Shared reuse
is typed evidence/provenance/identity, Spark/Kafka runtime patterns, S3 client,
committed-publication conventions and resource monitoring—not domain state logic.
