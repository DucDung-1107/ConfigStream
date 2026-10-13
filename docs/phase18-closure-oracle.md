# Phase 18.1 — independent RIS oracle

## Claim and boundary

The reference now independently decodes the required accepted-event projection
from raw MRT/BGP bytes. `workloads/ris_reference.py` imports neither production
expansion code nor `mrtparse`. The orchestrator retains the pinned parser's
**acceptance disposition**, but writes expected Parquet rows from the independent
decoder and compares every accepted occurrence's complete projection. Agreement
is not a proof of all BGP implementations or of the validity of excluded messages.

The research problem, three RQs and four roles remain frozen in
[Phase 15](phase15-research-freeze.md). This closes evidence for RIS-specific RQ1;
it does not establish four-domain generalization or streaming superiority.

## Evidence levels

| Level | Obligation | Mechanism |
|---|---|---|
| 1 | Independent framing, offsets and all-record indices | Separate bounded header/body reader; complete gzip EOF/CRC |
| 2 | Peer, source time, action, AFI/SAFI, prefix, path ID, expansion | Independent wire extraction; manual owned UPDATE/MP/ADD-PATH expectations |
| 3 | Required attributes and historical digest/fingerprint | Independent attribute boundaries and values; frozen JSON presentation |
| 4 | Exact complete committed logical projection | Spark bidirectional full-field differences and physical digest readback against level-3 Parquet |

Level 3 is reported per archive, not inferred from frame counts. Level 4 additionally
requires the selected real service run; an offline full-hour oracle alone cannot
close service acceptance.

## Attribute policy

Supported: ORIGIN, AS_PATH/AS4_PATH and segment order, NEXT_HOP, MED, LOCAL_PREF,
AGGREGATOR/AS4_AGGREGATOR, ordinary/extended/large communities, ORIGINATOR_ID,
CLUSTER_LIST, unicast MP_REACH/MP_UNREACH for IPv4/IPv6 and supported ADD-PATH.
The decoder consumes declared attribute boundaries exactly. Unsupported families
fail closed. Extended communities preserve their wire integer values without
claiming interpretation of every subtype. ATOMIC_AGGREGATE, Only-to-Customer and
AS_PATHLIMIT and type 255 retain opaque frozen presentation where appropriate;
no causal meaning is invented.

The labels, enum presentation and JSON shape implement an explicit compatibility
contract with `mrtparse 2.2.0`; **attribute values are decoded independently**.
The existing `SHA256(json.dumps(attributes, sort_keys=True))` contract is unchanged.
The descriptive fingerprint remains the original tuple/repr policy, including
source seconds and the attribute digest bytes. It is never an event identity.
Raw-wire bytes and parsed-attribute JSON would establish different checksums;
the latter has not been silently replaced by the former.

Primary format references: [RFC 4271](https://www.rfc-editor.org/rfc/rfc4271.html),
[RFC 4760](https://www.rfc-editor.org/rfc/rfc4760.html),
[RFC 6793](https://www.rfc-editor.org/rfc/rfc6793.html), and
[RFC 8092](https://www.rfc-editor.org/rfc/rfc8092.html).

## Exact ledgers and multiplicity

`workloads/ris_oracle.py` writes private Parquet in bounded 4,096-row chunks and
SQLite identity/fingerprint inventories. The window ledger uses a disk-backed
primary key on occurrence identity; a collision fails rather than disappearing.
Its ordered digest binds `[occurrence_id, SHA256(canonical complete row)]` with
the `ris-window-ledger-v1` header. Per-file source-order digests retain the previous
JSON-newline format. These are distinct digest definitions, not interchangeable.

For a window, repeats beyond first = occurrence count minus distinct fingerprints.
Within-archive repeats are summed per-file repeats. Cross-archive additional
repeats = whole-window repeats minus that sum. Cross-archive repeated keys are a
different statistic: fingerprints present in more than one archive. Owned two-file
fixtures establish 8 events, 2 fingerprints, 6 global repeats, 4 within-file repeats
and 2 cross-file additional repeats. No fingerprint deduplication is performed.

All 57 historical exclusions retain their original pinned disposition. Independent
zero-prefix extraction does **not** certify that those BGP messages are invalid.
Error coordinates, body error category and independent extraction status remain
separate. Full source ledgers and external normalized rows remain private.

## Reproduction

```sh
.venv/bin/python -m workloads.ris_oracle --files 12 \
  --output .cache/phase18-closure/oracle-hour --max-seconds 3600
```

This command requires fresh private output and cached pinned archives; it never
downloads. It checks free disk, output growth and its preregistered deadline.
Oracle execution time includes multiple integrity/independence passes and is not
a matched batch performance baseline. See [experiments](phase18-closure-experiments.md)
and [acceptance](phase18-closure-acceptance.md) for actual evidence scope.

## Preserved coverage failure

The first whole-window attempt stopped in archive `0020`, offset 28,237,739,
all-record index 152,669: independently decoding attribute type 21 was not yet
supported. The failed private attempt is preserved. Pinned production represents
AS_PATHLIMIT as opaque bytes; independent support adds its declared label and exact
bounded byte value, without claiming AS_PATHLIMIT semantics or changing any event,
identity, attribute digest, fingerprint or exclusion policy. A literal owned vector
tests the presentation. Completed first-four-archive ledgers have no type-21
attributes and can be reused only with their source and projection checksums
verified; no partially produced fifth-archive ledger is reused.

## Measured twelve-archive source result

All twelve archives reached level 3. Exact complete accepted projections agree
between independent raw-wire extraction and pinned production: **zero missing or
unexpected accepted identities**. Acceptance disposition remains qualified as
above. The private SQLite union independently verifies identity disjointness and
fingerprint accounting, not merely aggregate equality.

| Archive UTC | Accepted occurrences | Excluded source records |
|---|---:|---:|
| 00:00 | 370,141 | 5 |
| 00:05 | 431,043 | 5 |
| 00:10 | 502,663 | 4 |
| 00:15 | 323,489 | 5 |
| 00:20 | 333,186 | 5 |
| 00:25 | 368,821 | 5 |
| 00:30 | 355,915 | 4 |
| 00:35 | 340,518 | 5 |
| 00:40 | 412,731 | 5 |
| 00:45 | 462,461 | 5 |
| 00:50 | 438,102 | 4 |
| 00:55 | 386,086 | 5 |
| **Union** | **4,725,156** | **57** |

Framing totals remain 2,300,578 records / 2,291,545 UPDATE messages, with
8,976 accepted non-UPDATE/control records. Sources contain 73,771,296 compressed
and 427,068,595 decompressed bytes. All 57 exclusions retain exact framed
coordinates and the pinned `Insufficient buffer 0 < 1 byte` category; independently
extracted eligible prefix count is zero, without a message-validity claim.

The union contains 4,606,984 fingerprints and 118,172 repeats beyond first. Exact
SQL finds zero additional cross-archive repeats for this window's timestamp-bearing
fingerprint, not an assumption that cross-file repeats cannot occur. Owned fixtures
exercise that case. The first three archives contain 1,303,847 events and 31,283
repeats. Full-hour ledger SHA-256 is
`cc9e86c3e882351ef5fc85774e647783c5b7500b54aedfc4bd8cbf6b75ccdd5c`.
Source peak is 181,868/minute and fixed-hour mean 1,312.543333/s; neither measures
ConfigStream throughput. See the compact [ledger artifact](../workloads/results/phase18-closure/occurrence-ledger-validation.json)
for per-file source hashes and projection checksums.
