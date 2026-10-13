# Phase 18.1 — committed source and window integrity

## Controlled additive decision (2026-10-13, P18.1-M1)

Add `ris-accounting-1.0` and `ris-window-1.0` JSON metadata under the existing RIS
conditional metadata namespace. This does not change occurrence identity, event
columns, parser disposition, counting/fingerprint policy, registered event paths,
Terraform or RPKI formats. Existing `ris-1.0`/`ris-1.1` experimental commits remain
immutable and readable with their original guarantees. The
[prior correction](phase18-contract-amendment.md) remains authoritative.

## Boundaries and publication

| Object | Contents | What it establishes |
|---|---|---|
| `sources/<manifest_id>` | Existing pinned RIS reference | Immutable provenance, not delivered-event completeness |
| `exclusions/<artifact_sha256>` | Typed source-record exclusions | Exact error coordinates/categories, never accepted events |
| `accounting/<artifact_sha256>` | Counts, policies, collector, selection time and ledger digests | Selected archive accounting under the pinned policy |
| `batches/<query>/<batch>` | Existing `ris-1.1` physical inventory | Committed normalized batch, not complete source coverage |
| `windows/<query>` | Source descriptor digests, fixed batch inventory, logical count/digest | Verified selected accepted-event window, never routing state |

Publication writes reference and exclusion content **before** archive accounting;
window publication follows exact full projection and source-order verification.
`publish_accounting` checks typed exclusions, unique coordinates, source bindings,
count identities and content checksum. A descriptor alone is not proof that an
arbitrary caller independently checked the source: the experiment finalizer must
validate its oracle and committed rows first. This trusted-call boundary is explicit.

Archive selection time derives from a declared archive filename, separately from
actual MRT timestamps. Owned/unknown references keep unknown selection time. A
complete gzip partition is neither a complete route table nor a globally ordered
stream. Scope, artifact, policies and source provenance remain exact.

## Reader behavior

`read_accounting` revalidates the referenced source and exclusion ledger; missing,
corrupt or conflicting content fails closed. `read_window` verifies the fixed
committed inventory and all referenced source descriptor hashes. `completed_events`
additionally revalidates physical Parquet count/checksum, exact complete oracle
projection and final logical digest. Missing batch metadata can no longer silently
reduce a **complete-window** read. The original partial committed-event reader is
unchanged: it does not claim an entire source window is complete.

Publication remains single-writer conditional metadata publication, not ACID or
an atomic Kafka/Spark/MinIO transaction. A failed final window commit leaves valid
individual event commits readable, but no complete-window claim. Same batch retries
reuse committed metadata; later-batch retries may retain physical copies, while
logical reads reconcile occurrence IDs and reject conflicts. Distinct equal-looking
occurrences across offsets and artifacts survive.

After a complete window is sealed, exact source retries validate both incoming
oracle projections and existing complete physical history, then acknowledge without
adding physical commits. This keeps the fixed completion inventory valid. A new
source requires a new selected window; conflicting retries fail. Closed-window
recovery can reread history and is not the normal ingestion fast path.

## Recovery and safety

New isolated owned-fixture tests cover missing/corrupt source metadata, missing or
corrupt Parquet, absent/conflicting exclusion content, missing batch metadata and
absent window completion. Existing actual broker/callback tests cover interruption
before output, partial output, before commit and after commit before checkpoint
acknowledgment, plus invalid transport and older-coordinate conflicts. Execution
status is recorded separately in [acceptance](phase18-closure-acceptance.md).

Corruption tests may overwrite/delete **only their UUID-owned fixture prefixes**.
They never mutate prior RIS or RPKI evidence. Normal CI uses owned fixtures and no
live download. Callback exceptions do not prove power-loss, broker failover,
multiwriter transactions or retention-loss durability.

Rollback disables new experiment commands/metadata use. No configuration or RPKI
migration is required; keep all existing immutable evidence and volumes.

## Bounded digest decision (2026-10-13, P18.1-M2)

The first fifteen-minute completion attempt exposed Java heap exhaustion while
retrieving a sorted partition for the final digest. Use explicit range partitions,
distributed maximum-row validation and non-prefetched local iteration, retaining
the exact original ordered JSON/SHA stream. This changes execution only: no new
identity, parser, fingerprint, physical schema or historical checksum policy.
Range sampling may produce skew, so the maximum-row guard rejects unsafe output
before iteration. Small independent ordering/skew tests and actual owned-service
regressions pass; a fresh larger real-source completion still requires approval
and successful acceptance. See [negative measurements](phase18-closure-experiments.md).

## Full-hour failure barrier

The later authorized twelve-file run failed global hash aggregation in the outer
`complete=True` identity check. Its 151 physical batch checks and source bindings
had completed, but complete accepted-event accounting did not. Finalization
therefore did not publish archive accounting, exclusion sidecars or window metadata.
The twelve source references and physical commits remain retained as partial
history; they cannot establish complete selected-window coverage. A lag of zero
does not override this barrier. Bounded digest iteration alone does not solve the
separate global aggregation capacity issue.

The fourteen owned actual-service recovery cases passed again in 134.84 seconds
before the run. The full-hour attempt was not retried or repaired in place. Its
objects and checkpoint are preserved for inspection; any proposed repair must
retain the same identity/oracle and immutable-publication guarantees.
