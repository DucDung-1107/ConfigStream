# Phase 17 — RPKI architecture and controlled registration amendment

Starting clean `main`: `2d5772db73feba03376572f7dc6d9ef000112697`.
Owner authorization covers this bounded implementation, not instructor approval,
raw redistribution or installation of container infrastructure.

## P17-2026-10-12-RPKI-MEMBERS

Phase 16 descriptors cannot physically hold a complete daily membership inventory.
Add an explicit **RPKI physical registry**, separate from the bounded Phase 16
fixture publisher: source-row ledger, canonical members, completion descriptor,
transitions and boundary descriptor. Its version is `rpki-1.0`; the existing
four temporal registrations and all configuration paths/formats stay unchanged.
Source rows use a domain-specific transport contract, not a ConfigSnapshot or a
new universal envelope. Existing Phase 16 observation/transition Evidence remains
the logical descriptor/transition contract. This amendment adds storage/transport
granularity, not a new research question or new authorization-change semantics.

P17-ID-v1 (same date): a raw occurrence additionally binds the observation ID.
Identical archive bytes on different dates are separate observations, not retries.
Raw batch checksums cover the full physical projection, including provenance,
not merely row ID/payload hash. Private pre-release pilots lacking the final
`full-source-row-v1` commit policy are not accepted final ledgers; retain them as
prototype evidence and use a new query ID rather than rewriting their commits.

| Physical dataset | Identity / fields | Publication and query contract |
|---|---|---|
| Source archive | scope, date, compressed SHA-256, URL, size | Private immutable cache, verified before reading; retained once, not copied into Parquet |
| Source rows | artifact SHA, 1-based CSV data-row ordinal, row ID, membership ID, canonical tuple/encoding, payload digest, provenance ID | Committed micro-batch ledger, duplicate delivery reconciled across batches; source archive + coordinate reproduces omitted URI/certificate columns |
| Canonical members | observation ID, scope/date, ASN/prefix/maxLength, membership ID, canonical encoding | Unique members; complete-day manifest freezes exact member files; no driver-side member collection |
| Completion | Phase 16 Evidence + SourceManifest, exact source counts and set digest, member-path inventory | Published last; absent completion means unavailable historical state, not empty state |
| Transitions | previous/current observation IDs, action, canonical member, logical transition ID | Only complete comparable consecutive observations; distributed anti-joins |
| Boundary | both observation IDs, exact transition count/digest and paths | Published even for zero changes; reconstruction rejects a missing boundary |

Paths are under `temporal/rpki/`, with isolated `_rpki_commits/v1/` metadata.
Identifiers in path segments are validated SHA-256/UUID/integer values. Private
uncommitted outputs are not scanned by historical readers. Member partitions use
observation identity (one scope/date per state), not ASN or prefix. Micro-batch
paths additionally include query ID and batch ID. File size is bounded by the
existing lake `max_records_per_file` option. No legacy checkpoint is reused.

Completion and boundary descriptors are small conditional JSON metadata, at
`_rpki_commits/v1/state/<scope>/<date>.json` and
`_rpki_commits/v1/boundary/<previous-id>/<current-id>.json`. Their member/transition
file inventories are separate. The inherited observation registration is reserved
but no redundant descriptor Parquet copy is materialized. The writer uses an
explicit 100,000-row file bound; it does not change the legacy writer's options.

## Planned operational path

Pinned XZ CSV → verified bounded source chunks → `temporal.rpki.source.v1`
→ dedicated Spark `foreachBatch` → committed raw ledgers → canonical daily members
→ independently verified completion → consecutive transitions → reconstruction.

The new topic is explicit/opt-in; old `config.*` topics are unchanged. Key is the
observation ID; stable chunk identity recognizes retries, while each raw row has
its own source coordinate. Retention must cover the whole run plus recovery;
no broker setting is silently changed. Producer progress advances only after
acknowledged chunk flush; crashing before progress persistence may resend a chunk.
Logical reconciliation, not Kafka idempotence alone, resolves that retry.

The independent SQLite oracle stores sorted canonical encodings on disk and
exports private Parquet inventories. Spark validates exact sets with distributed
anti-joins, not just aggregate counts. Compatibility checksum ordering is globally
sorted canonical UTF-8 bytes; a streaming iterator hashes length-framed bytes with
bounded memory. This is a serial digest bottleneck, not a distributed hash claim.

Single-writer conditional commits follow existing MinIO conventions. A batch
commit, Spark checkpoint acknowledgment and daily completion are distinct. No
cross-system atomic transaction or global exactly-once guarantee is asserted.
Implementation/testing status is in [acceptance](phase17-acceptance.md).
