# Phase 18.2 — exact bounded reconciliation

## Selected method

Use Option B: hash-partition the tagged union of actual and independent expected
EventRows by **logical_record_id**, then sort each partition by that identity.
This is global identity partitioning, not partitioning by a possibly incorrect
artifact field. Spark handles sort/spill; Python retains one full projection and
constant flags for the current identity. Arbitrarily long duplicate groups do not
accumulate in Python memory. Validated output is persisted DISK_ONLY for the
duration of finalization and released in a context manager.

For each identity, compare all 18 EventRow fields with exact typed equality,
including nullable time/path fields. A differing row marks a conflict. Presence
only in the oracle marks missing; presence only in actual marks unexpected.
Presence on both sides with one identical projection yields one accepted row.
The oracle precondition is the frozen independent unique identity ledger.

Every row with an identity uses the same hash partition regardless of artifact,
physical batch or arrival order. Sorting makes that identity contiguous. Thus a
conflict cannot escape through an archive or input partition boundary. Different
identities with equal fingerprints remain separate. The only collected summary
has at most four status counts; the diagnostic aggregation has four keys, not
millions of occurrence keys. Partition bounds are explicit (2–256, default 64).

## Physical readback and checksums

Each immutable committed batch still undergoes source-reference validation and
its original count/ordered projection digest check. Afterwards one multi-path
Parquet read replaces the 151-way DataFrame union. One bounded complete comparison
replaces nested whole-history deduplication and repeated reconciliation. The old
`exact_projection` remains for bounded callbacks and equivalence tests.

Existing global SHA-256 bytes remain unchanged: sorted logical IDs, canonical
sorted-key JSON of full EventRows, newline separators. Source-order SHA also
retains `(byte_offset, expansion_ordinal)` order. Partition SHA concatenation is
not used. Owned checksum vector:
`4a4c5f1bc228e15f37f852d0e70df5fb87e6c80f35504194aa87d51c5ee5479a`.

Sorting can spill but does not guarantee every workload fits every heap. Medium
tests and real recovery are separate capacity gates. Accepted digests are
supported by full row equality, rather than replacing it.

## Recovery boundaries

`benchmarks.ris_service --finalize-only` reads the earlier namespace. It cannot
publish Kafka events, write batch Parquet, rewrite batch commits or advance a
Spark checkpoint. Approval binds query/prefix, pinned source hashes, frozen
inventory checksum, exact checkpoint fingerprint and declared time cap.
Checkpoint offsets must agree with the earlier drained source evidence.

Before final metadata publication: verify immutable physical batches, complete
identity/projection agreement, all per-source counts/source-order digests, global
SHA, and historical queries. Compare the private 57-row exclusion inventory with
the frozen public coordinate audit. Then conditionally publish exclusion sidecars,
source-accounting descriptors and finally the complete-window descriptor.
Inventory and checkpoint integrity are checked throughout. A readonly mode
performs the same checks without metadata publication.

Interruption after a source descriptor may leave valid metadata without a window.
Retry recognizes identical metadata and rejects conflicts. The single-writer
assumption remains; this does not add a cross-system transaction. Legacy
`ris-1.0/1.1`, window/accounting schemas, source identities, fingerprints,
Terraform and RPKI layouts retain their meanings. See [tests and scope](phase18-finalization-experiments.md).
