# Phase 16 — identities, completeness and independent oracles

## Deterministic policy v1

ID = SHA-256 of UTF-8 JSON `["configstream-temporal-v1", kind, policy, tagged_components]`.
Each scalar is explicitly tagged `string`, `integer`, `boolean` or `unknown`.
Strings are NFC, JSON object keys sorted, separators compact, no floats/NaN,
ambiguous concatenation, out-of-range integer or normalized-key collision.
Bounds: depth 16, strings 65,536 chars, sequences 10,000, canonical output 1 MiB.
IP values use canonical addresses/networks (host-bit prefixes reject, not repair);
ASNs are strict uint32, maxLength is validated against family/prefix length.
Dates use calendar ISO dates; aware times normalize UTC while retaining originals.

The **new implemented namespace binding** hashes parser, canonicalization and
counting-policy versions into a policy namespace. This elaborates Phase 15's
policy-change requirement: changing any binding changes identity rather than
silently changing payload under an old ID. This is Phase 16's pre-measurement
contract refinement, not an amendment to old Phase 13 occurrence counts.
The [registry](../workloads/results/phase16/contract-registry.json) records it.

| Kind | Auditable components (in addition to kind/policy/source/scope) |
|---|---|
| Configuration reference | Existing ConfigSnapshot ID and Phase 12 sidecar; **unchanged** |
| RPKI observation | Artifact hash, date, canonical/comparison policy, validation regime |
| RPKI membership | Scope, integer ASN, strict canonical prefix, maxLength |
| RPKI transition | Both observation IDs, membership ID, action, comparison policy |
| DNS raw observation | Artifact hash, Parquet row-group and row; not address-set equality |
| RIS occurrence | Artifact hash, decompressed header offset, expansion ordinal; all-record index is an audit field |

Canonical preimages can be reproduced from safe coordinate/policy metadata.
Reconciliation compares complete validated evidence for equal IDs: a conflicting
payload, coordinate, time or provenance fails. Arrival time lives in `Delivery`,
not identity. Equal DNS values at different rows and equal RIS payload/time at
different offsets remain distinct. Different observation dates remain distinct,
even if membership/state content is equal. No payload-only dedup.

## Time honesty

Point/interval/date/unknown are separate tagged types. Point original encoding
and normalized value must agree; wrong epoch units/naive times fail. DNS
millisecond encoding can retain second precision. RPKI dates are not midnight
operator timestamps. RIS times are collector evidence, not guaranteed wire-causal
timestamps. Ties and inverted source times are not repaired or totally ordered
by fabricated timestamps. Ingestion never fills missing source time.

## RPKI barrier and owned oracle

A descriptor contains scope/date/artifact, policy/regime, expected/accepted/
excluded/duplicate/unique counts, set digest and complete/incomplete status.
For the initial conservative policy, complete requires verified source accounting,
all accepted rows and zero exclusions; malformed input cannot become a removal.
Canonical duplicate rows collapse only in the membership set, not raw accounting.
The supplied members must match scope, counts and digest before state use.

Digest framing is `rpki-set-v1\n`, then sorted unique UTF-8 arrays
`[scope,asn,prefix,maxLength]`, each prefixed by uint32 big-endian byte length.
The literal one-member digest is independently authored in
[vectors](../workloads/results/phase16/identity-test-vectors.json). Owned expected
sets/additions/removals and baseline-plus-difference reconstruction are specified
without calling the function under test to construct expected sets.

Consecutive calendar days, same source/scope/frozen collection, parser,
canonicalization/counting/comparison policies and regime are required. Unknown
archive-reported regimes are only qualified same-window comparisons, not claims
of equivalent validators. A missing, partial, truncated, checksum-mismatched or
incomparable day blocks the edge; start a new baseline segment later. Completion
after restart can validate the originally pending state; a conflicting committed
same-day descriptor cannot overwrite history. Future real 14-day oracle is pending.

## DNS uncertainty oracle

Complete artifact accounting does not certify entity observation coverage.
`nonempty`, independently `proven_empty`, timeout/failure and unknown/absent
are distinct fixture statuses. Proven empty requires a query-evidence reference;
it is a claim that a future adapter/oracle must verify, not a constructor inferring
empty from missing A/AAAA rows. An `absent` placeholder cannot be published as a
raw Parquet measurement: it has no source row coordinate. Failed measurements
are preservable evidence but cannot establish a deletion/difference.

The owned comparison requires consecutive partitions, successful qualified
address evidence, matched query/response types, collection/comparison policies
and worker qualifiers, plus an explicit partition-complete barrier. Both unknown
workers remain explicitly unknown, not proven same-vantage. Value removals from
a proven empty fixture are **observed-value differences**, cause unknown, never
authoritative DNS deletions. Missing chunks, changed workers or late unfinished
partitions block comparison. Future OpenINTEL oracle/SQL and query-level empty
classification remain Phase 19 gates; no confidence probability is invented.

## MRT offset hard gate

[RFC 6396 sections 2–3](https://www.rfc-editor.org/rfc/rfc6396) specify the 12-byte
common header and declared body length; extended types add a uint32 microsecond
offset included in that length. The independent framer advances by exactly
12+length, retaining all-record index including unsupported bodies. Default body
bound 1 MiB (explicit allowed maximum 16 MiB), one frame buffered at a time.
Short header/body, excessive length or too-short extended timestamp fails at a
safe coordinate; no heuristic resynchronization. Framing alone cannot certify
BGP message validity or identify a body whose declared length is dishonest.
The stream must expose its decompressed position and begin at artifact origin;
a pre-seeked/sliced stream is rejected rather than relabeled offset zero.

[Pinned mrtparse 2.2.0 implementation](https://github.com/t2mune/mrtparse/blob/v2.2.0/mrtparse/__init__.py)
does not expose occurrence offsets. On our owned unknown type it raises `KeyError`
before consuming the body. Therefore coordinates are assigned **before** parsing:
each frame is decoded in isolated bounded bytes, and parser exceptions retain the
independent frame coordinate/category rather than shifting subsequent frames.
This prevents parser behavior from defining accepted-only record indices.

Owned hex fixtures manually specify offsets 0/63 for equal UPDATEs; unknown then
valid 0/14; malformed body then valid 0/13; MP/ADD-PATH frames 0/109/180.
Literal expansion oracles cover conventional multiple NLRI, IPv6 MP_REACH/
MP_UNREACH and ADD-PATH path IDs. Order is conventional withdrawals, conventional
announcements, MP attributes in wire order, prefixes in wire order; ordinals are
assigned before delivery and no equal prefix occurrence is suppressed.

The cached `updates.20260901.0000.gz` hash
`2f6c7a5af25b310f3cef4118e4b8f9acf2b7896f3a00b040f7ef4e662ec1ea8d`
matches Phase 13. The bounded proof checks the first **1,000 framed records**,
**160,268 decompressed bytes**, **1,000 parsed / 0 excluded** in this prefix.
No source downloads. Full compressed file hash is checked, but partial gzip
read does not validate decompressed EOF/CRC or full-hour coverage.
See [measured proof](../workloads/results/phase16/ris-coordinate-feasibility.json).

Decision: **RIS_OFFSET_IDENTITY_PROVEN**, scoped to framing/owned fixtures/cached
prefix, not complete semantic parsing. No all-record-index fallback is needed.
Full-hour coordinate expansion, session-dependent cases, unsupported records and
parser coverage remain Phase 18 gates. The old **57 excluded parser records** and
**118,172 repeated fingerprints** are unchanged; the new prefix has no authority
to reinterpret them. No RIB/route-state reconstruction is claimed.

## Oracle independence and limitations

Identity vectors use literal preimages and a separately authored SHA-256 calculation;
tests do not compute expected hashes using production identity functions. Set
fixtures have explicit expected members/checksum. MRT byte positions and NLRI
lists are manually specified. The cached check shares mrtparse with Phase 13;
it independently verifies **framing**, not all parser semantics. Publication
fixtures are generated owned records for recovery, not independent ID oracles.
Actual local Parquet tests check visible reconstructed evidence; mocked commit
calls do not establish service-level conditional-put correctness or durability.
