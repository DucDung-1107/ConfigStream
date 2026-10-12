# Phase 17 — RPKI correctness

This implements the RPKI role of [Phase 15](phase15-research-freeze.md), using
[Phase 16 contracts](phase16-identity-and-oracles.md), without changing Terraform,
DNS or RIS processing. The physical registration amendment is in
[architecture](phase17-rpki-architecture.md).

## Two independent implementations

`workloads/rpki_oracle.py` uses standard-library CSV, IP validation and disk-backed
SQLite sets. It does not import the production canonicalizer, temporal models or
Spark transformations. It independently spells out the membership identity
preimage, validates every pinned archive, exports private member/source-coordinate
Parquet ledgers, calculates SQL `EXCEPT` differences, and applies every boundary
to a reconstructed SQL set. SQLite uses a 16 MiB page-cache budget and disk temp
storage. Source exports use 4,096-row Arrow batches.

`spark/rpki.py` validates typed source chunks on executors, reconciles source
occurrences, forms unique membership DataFrames, and performs exact bidirectional
anti-joins against the independent inventories. Equal total counts alone cannot
complete a day: the exact `(row_index, member_encoding)` map and complete member
projection must match. The oracle is intentionally a prerequisite for this
research acceptance path, not a claim that production needs two full parsers.

Independent fixtures specify actual UTF-8 preimages, manually selected expected
members, dates and actions. Owned fixtures include duplicate memberships and IPv6.
Both implementations use Python's `ipaddress` library: correlated library errors
remain a limitation, mitigated by explicit IPv4/IPv6 boundary fixtures. They are
not two independent cryptographic RPKI validators. No certificates are revalidated.
Oracle summaries attach observation/provenance IDs using the previously tested
Phase 16 reference contracts. That binding helper is reused, not claimed as an
independent second implementation of every identity function. The source sets,
row maps, member preimages and SQL differences remain independent of Spark.

## Identity, counting and checksum policies

- Membership: `(scope, ASN, canonical network prefix, maxLength)`, using frozen
  `membership-v1`. ASN is an integer in `[0, 2^32-1]`; maxLength is between prefix
  length and the address-family maximum. Source CSV prefixes must be canonical.
- Raw occurrence: the existing typed SHA-256 identity utility binds source,
  scope, **observation ID**, compressed artifact SHA-256 and 1-based data-row
  ordinal. Its policy namespace binds parser, canonicalization, counting and
  `observation-bound-row-v1`. URI/certificate fields are recoverable from the
  retained source archive and row coordinate, not fabricated from a set member.
- Observation: existing Phase 16 scope/date/artifact/policy identity. Identical
  members, or even identical archive bytes, on different dates remain different
  observations. Different rows with equal members remain different raw evidence.
- Transition: existing Phase 16 identity binds previous/current observation IDs,
  canonical member and action. Native Spark expressions reproduce its preimage;
  an independently specified test checks that relationship.
- Canonical set digest: SHA-256 initialized with `rpki-set-v1\n`; each sorted
  canonical JSON tuple's UTF-8 bytes are prefixed by their 4-byte big-endian length.
- Boundary member digest: `rpki-transition-members-v1\n`, additions before removals,
  canonical tuple sort order within each action, length-framed `action:tuple`.
- Source map digest: `rpki-source-map-v1\n`, ordinal-sorted, length-framed JSON
  `[row_index, member_encoding]`. Raw batch digest additionally covers **all**
  physical source/provenance fields under `full-source-row-v1`.

Reconciliation suppresses only equal source-coordinate redelivery. Same logical
ID with different content or context fails the batch. Physical duplicate copies
across batches may exist; logical readers reconcile them. No payload-only dedup,
global exactly-once claim or cross-system atomic transaction is made.

## Completeness is a separate barrier

The oracle's input reference describes an expected complete source artifact; it
is **not** a published completion. A day is published only after all source row
coordinates `1..N` are present exactly once logically, bindings and policy match,
the independent row map and member set agree, and counts/digest are verified.
Member files are written first; the immutable completion descriptor is published
last. Missing rows return pending. Invalid bindings, accounting or digest fail.
No complete descriptor means no historical state, not an empty state.

Completeness means the entire selected publisher CSV, **not** proof of globally
complete trust-anchor coverage, all ROAs or administrator intent. The validation
regime string declares this experiment's frozen archive comparison assumption.
Publisher metadata does not establish the precise September 2026 validator build.
Scope or regime changes are incomparable, not normal removals.

Transitions require both independently accepted, committed states, equal scope,
source/dataset/policies/regime and consecutive calendar dates. Missing days are
not bridged. Revised same-day artifacts conflict rather than overwrite history.
Boundary descriptors also exist for zero changes. Baseline/delta reconstruction
requires every ordered boundary; missing boundaries fail. Every intermediate
reconstructed set is checked against that day's declared digest.

Daily dates retain observation-date semantics. They are not precise operator
action, deployment or network-effect timestamps.

## Failure evidence matrix

`OFFLINE` includes actual local Spark/file-checkpoint tests, not remote services.
Live Kafka/MinIO fault injection is **NOT_RUN** in this environment.

| Required failure | Offline evidence | Live service status |
|---|---|---|
| Missing source file | source/oracle rejection | N/A: local source |
| Wrong archive SHA | both parsers reject | N/A |
| Malformed CSV / ASN / maxLength / decoding | pinned bad owned CSV rejection | N/A |
| Duplicate row delivery | same coordinates reconcile | NOT_RUN |
| Duplicate membership within day | owned 4 rows → 3 members | NOT_RUN |
| Equal membership on different days | distinct observation/row identities | NOT_RUN |
| Missing final chunk | pending, no completion | NOT_RUN |
| Corrupt expected count | reference/inventory rejection | NOT_RUN |
| Incorrect state digest | completion rejection | NOT_RUN |
| Nonconsecutive dates | oracle/comparator refuse gap | NOT_RUN |
| Wrong scope | reference/row binding rejection | NOT_RUN |
| Different comparison regime | comparator refuses | NOT_RUN |
| Same-day revised source | immutable conflict / oracle SHA conflict | NOT_RUN |
| Spark restart during processing | real owned file-source checkpoint retry | Kafka NOT_RUN |
| Retry committed batch | exact metadata and logical rows reused | MinIO NOT_RUN |
| Partial output, no manifest | local reader refuses unpublished data | MinIO NOT_RUN |
| Commit before checkpoint acknowledgment | local Structured Streaming interruption/restart | Kafka/MinIO NOT_RUN |
| Query incomplete day | rejected | MinIO NOT_RUN |
| Missing reconstruction boundary | rejected | MinIO NOT_RUN |
| Existing configuration regression | full old unit/API/Spark suites | old service tests NOT_RUN |

Malformed transport is quarantined as batch ID, chunk count and error class,
without source payload. The batch/checkpoint offsets and retained input identify
the failed input for audit; it is not counted as an accepted event. This strict
reference policy accepts zero CSV exclusions. A new exclusion policy would need
an explicit amendment and a new oracle, not a silently adjusted expected total.

## Limits of recovery claims

Local conditional metadata uses fsynced temporary files and an exclusive hardlink.
MinIO uses the existing S3 client with `IfNoneMatch="*"`, exact retry comparison
and bounded metadata. These mechanisms are **single-writer**, not a new ACID
protocol. Data files, metadata commit, daily completion and checkpoint acknowledgment
are separate. Offline tests cannot establish remote conditional-write behavior.
The opt-in service fixture tests actual Kafka/MinIO when available; unexecuted
fault cases remain gates. See [acceptance](phase17-acceptance.md).
