# Phase 19 — Committed visibility and recovery

The existing single-writer MinIO conditional metadata model is reused, with an
isolated `_dns_commits/v1` namespace. Existing Terraform/RPKI/RIS formats and
checkpoints are unchanged. There is no global ACID, universal exactly-once or
multiwriter claim.

Raw callbacks validate the full source-coordinate projection against the
independent oracle. Exact repeated source IDs reconcile; equal values at distinct
coordinates survive. Conflicting fields at one ID fail, including worker/time
changes. Immutable committed batch retry compares metadata and does not rewrite
published output. Sealed-source exact replay is a validated no-op and cannot
invalidate the existing completion inventory. Mixed sealed/unsealed new batches
are rejected rather than silently changing the completion boundary.

Readers enumerate committed metadata, not uncommitted directories. Each raw
batch's count, full-projection digest and source manifest is checked before logical
readback. Each analytical table has a versioned schema, row count and sorted digest.
Completion metadata binds source-scoped batch inventory and the four group/value/
reuse/contributor commit hashes. Missing physical data or corrupt projections fail
closed. Reused-value reconstruction is compared exactly, never trusted by size.

Owned fixture tests inject callback failure before output, after partial output,
before metadata and after commit before checkpoint acknowledgment. Actual Kafka
queries restart the same checkpoint and verify final raw identities and group
references. These are callback-exception tests, not power-loss durability or broker
failover proof. Additional actual-MinIO tests use disposable owned prefixes for
missing/corrupt sources, missing/corrupt Parquet, missing groups, corrupted values,
older source replay, conflicts and analytical completion interruption. They never
modify previously accepted external-source evidence.

Incomplete sources cannot complete groups. Group files committed before a failed
completion step do not create an accepted source claim. Late completion retries
verify immutable table content, then publish the completion descriptor conditionally.
Two-source differences require both completed observations and exact independent
comparison-ledger agreement; resolver/vantage qualification still remains unknown.

Actual service, owned-fixture and real-source recovery results are separately
identified in [validation](../workloads/results/phase19/validation.json).
Skipped service cases are never counted as pass. See [acceptance](phase19-acceptance.md).
