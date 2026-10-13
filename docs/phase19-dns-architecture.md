# Phase 19 — DNS measurement architecture

Entry is **GO — PHASE 18.2 ACCEPTED** at
`eaa8fdaaa16ade4a5fe21beb40a033c31c9cebae`. See
[entry policy](../workloads/results/phase19/entry-and-execution-policy.json) and
[additive contract amendment](phase19-contract-amendment.md).

## Flow and physical boundaries

The two pinned private Parquet objects → bounded PyArrow original-coordinate
importer → DNS-only typed Kafka chunks → dedicated Spark `foreachBatch` → immutable
MinIO raw batches → exact per-source analytical finalization → qualified groups,
contributor references and reusable address values → descriptive comparison.
This is not a zone snapshot, configuration diff, routing event or RPKI state.

| Dataset | Physical registration | Identity / boundary |
|---|---|---|
| Source references | `_dns_commits/v1/sources` | Phase 16 immutable manifest ID; original archive retained once privately |
| Raw measurements | `temporal/dns/measurements/physical-v1.1` | Policy-bound artifact / original row group / row; immutable query / batch commit |
| Observed groups | `temporal/dns/observed-groups/physical-v1` | Artifact / selected date / four-field query-response key |
| Contributor links | `temporal/dns/contributors/physical-v1` | Group ID and individual source-row ID; multiplicity preserved |
| Canonical values | `temporal/dns/value-sets/physical-v1` | Structured canonical address-array SHA-256, never raw-row identity |
| Reused groups | `temporal/dns/reused-groups/physical-v1` | Group qualifiers plus value-set reference, no discarded contributors |
| Descriptive differences | `temporal/dns/observed-differences/physical-v1` | Ordered source-pair identity; complete four-key comparison ledger |
| Qualification | `temporal/dns/comparison-qualifications/physical-v1` | Common group plus both observation IDs; vantage unverified |
| Completion | `_dns_commits/v1/completion`, `comparison` | Exact projection verification precedes conditional metadata publication |

Raw physical rows preserve original names/types, epoch integer, worker, status,
address columns, TTL, RTT binary64 hex, section, extended error, provenance and
the canonical SHA-256 of the entire original 99-field row. Retaining the pinned
original objects is necessary to reconstruct nonprojected record-specific fields.
The normalized projection is not advertised as an independent full-source backup.

Groups retain canonical address sets, contributing worker IDs, timestamp min/max,
unknown flags and contributor counts. Links identify each original observation.
Values are reused within each artifact; no optimal cross-day compression claim.
Files are controlled batches, not one object per measurement. Current publication
uses two output partitions per dataset/batch. Existing lake paths are unchanged.

## Transport and processor

Topic: `temporal.dns.source.v1.<UUID>`, two partitions, seven-day retention.
Schema `dns-source-1.1`, at most 128 rows and 1 MiB per chunk, source artifact as
message key: one artifact has partition-local delivery order, not universal order.
One-MiB producer buffering, broker acknowledgments and atomic private progress.
Restart re-verifies the pinned source and original coordinate stream. A lost
acknowledgment can replay coordinates; idempotent producer settings are not the
logical correctness proof.

Spark uses an isolated checkpoint, AvailableNow, at most 256 Kafka chunks per
callback. Original row-group coordinates are assigned before Spark, never inferred
from shuffled Parquet positions. Exact sorted identity/projection reconciliation
uses disk-only intermediates and controlled partitions; driver collections contain
only bounded source ranges and four validation categories. No millions-row collect.
The oracle coordinate range is narrowed for callbacks; full exact validation occurs
at artifact finalization. Equal payloads are not raw deduplication keys.

Conditional metadata publication assumes one writer. Physical output, immutable
batch metadata, checkpoint acknowledgment, source completion and comparison
completion are different boundaries; no cross-system ACID claim. Readers discover
only committed paths and verify counts, full-projection digests and provenance.
Partial evidence may be readable without a complete-source claim.

See [uncertainty](phase19-dns-uncertainty.md), [oracle](phase19-dns-oracle.md),
[experiments](phase19-dns-experiments.md), [recovery](phase19-dns-recovery.md) and
[acceptance](phase19-acceptance.md).
