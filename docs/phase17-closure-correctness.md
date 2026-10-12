# Phase 17.1 — correctness and compatibility boundaries

The independent SQLite oracle, pinned fourteen archives and all Phase 17 results
remain unchanged. The frozen window is September 1–14, 2026, `ripencc.tal`.
Expected reference: 5,062,923 source rows, 367,204 union members, thirteen boundaries,
7,096 additions and 4,920 removals. These are oracle references, not manufactured
pipeline answers; actual executed stages are in [acceptance](phase17-closure-acceptance.md).

## Two identities and three commit boundaries

Raw row identity binds source/scope, observation ID, compressed artifact hash and
one-based CSV row coordinate under the unchanged policy. Canonical membership
binds scope, ASN, canonical prefix and maxLength. Two equal tuples at different
coordinates remain two raw observations; equal daily state on another date is
another observation. Only equal source occurrence redelivery is reconciled.

A raw batch commit is neither Spark checkpoint acknowledgment nor complete daily
state. Readers use registered committed paths, not directory discovery. Partial
output remains invisible. Completion requires exact independent coordinate/member
maps, source binding/accounting and sorted canonical digest; transitions require
two complete comparable consecutive dates. Missing chunks cannot create removals.

## Optimization acceptance oracle

The reused DataFrame has already passed typed chunk validation, incoming exact
source-map anti-join, existing committed-segment checksums and full source-identity
conflict reconciliation. A count only selects which days merit expensive verification.
Before any complete descriptor, the physical committed ledgers are reread and
fully verified again. The original `finalize_day`, `publish_boundary`, `read_state`
and `reconstruct` algorithms are not weakened or replaced.

Owned Spark tests compare both modes on reordered chunks, an older-coordinate
retry, a commit-before-ack interruption and a fresh processor instance. They
require unchanged committed files, exact source row inventories, complete daily
sets, boundary actions and reconstruction. A distinct-payload retry fails before
publication. A deliberately corrupted newly written physical ledger must fail the
completion barrier even though a correct merged DataFrame remains cached.

## Additional independent transition identity audit

`workloads/rpki_identity_audit.py` reads the original SQLite oracle **read-only**.
SQL `EXCEPT` supplies additions/removals independently of Spark. A separately
written SHA-256/preimage implementation binds source scope, parser/counting policy,
previous/current observation IDs, membership and action. It imports neither
Spark transforms nor production identity helpers. It does not change the source
oracle, source selection or any historical result.

Owned tests use manually specified member/policy preimages and known fixture
add/remove tuples, and reject missing, extra, duplicated or changed identities.
The real audit reads only bounded committed transition Parquet files, compares
the complete `(logical_record_id, action, member_encoding, previous_id, current_id)`
sets and rejects duplicate physical occurrences. The independently calculated
sorted identity digest must also match the commit metadata. This is an exact
identity-level comparison, not just equal totals. The 20,000-transition bound is
an audit guard, not a source-scale or pipeline limit; external raw rows are not
copied into committed reports. Results are in
[transition validation](../workloads/results/phase17-closure/transition-pipeline-validation.json).

Example reference invocation (with an existing read-only oracle and validated
serialized day references):

```python
import sqlite3
from workloads.rpki_identity_audit import transition_ledger, validate_transition_ledger

with sqlite3.connect("file:.cache/phase17/oracle/oracle.sqlite?mode=ro", uri=True) as database:
    expected = transition_ledger(database, previous_reference, current_reference)
result = validate_transition_ledger(expected, committed_transition_projection)
```

`committed_transition_projection` comes only from the paths named in the accepted
boundary manifest; do not scan temporary directories. The audit supports this
canonical ASCII RPKI projection, not arbitrary Unicode domains or all temporal
record kinds. No causal policy-intent ground truth is inferred.

## Test evidence levels

| Concern | Offline/local test | Actual service test |
|---|---|---|
| Older retry/conflict across callbacks | Baseline and reuse modes, owned source maps | Broker checkpoint replay and conditional metadata |
| Partial input | Pending day; missing coordinate cannot complete | Interrupted member/raw publication; no complete state |
| Physical checksum | Corruption after raw publication rejected | Real S3 readback, exact source inventory and state barriers |
| Uncommitted visibility | Exclusive local metadata and checksum fixtures | Partial S3 output before conditional commit |
| Recovery | Fresh processor after raw commit and file-source checkpoint | Kafka checkpoint restart at eight declared failure points |
| Reconstruction | Missing boundary/gap/policy mismatch and exact owned state | Real committed state and boundary reads in executed stages |
| Legacy compatibility | Existing unit/API/Spark suites | Isolated legacy-reader check; existing manifest read where available |

Status and actual counts are in the machine-readable
[invariant coverage](../workloads/results/phase17-closure/invariant-coverage.json).
Mocked producer ACKs are not broker tests, local files are not MinIO tests, and
unit cases are not source records. No cross-system atomic or global exactly-once
transaction is claimed. Recovery is logical correctness under a declared
single-writer immutable-source model.

## Audit and rights

Archive SHA/size verification is independent of Spark. The SQLite oracle uses
separate source parsing, SQL set difference and ordered source/member digests.
Both parsers share `ipaddress`; neither revalidates RPKI certificates. Coverage
means the selected publisher CSV, not all global authorizations or operator intent.
The comparison regime token pins the project policy for this window; it does not
independently verify the publisher's exact 2026 validator build or establish that
no external validation-policy change occurred. Report exact CSV-set differences,
not inferred operator actions or certificate-validation correctness.

[RIPE's archive description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md)
identifies daily trust-anchor VRP CSVs and historical validator/schema issues.
[RIPE's copyright terms](https://www.ripe.net/about-us/legal/copyright-statement/)
describe research/educational use and attribution; third-party rights remain
separate. No unrestricted normalized/raw redistribution permission is inferred.
Only aggregate counts, hashes, owned fixtures and code are committed. Source
rows, binary archives, checkpoints, service logs and credentials remain private.
