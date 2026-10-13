# Phase 18 — identity, expansion and qualified oracles

## Three identities

| Concept | Definition | Use |
|---|---|---|
| Source record | Source/collector + compressed artifact SHA + decompressed header offset | Physical evidence position; all-record index is supplementary |
| Occurrence | Existing Phase 16 typed SHA-256 preimage, record kind `ris_occurrence`, policy binding, source/scope/artifact/offset/expansion ordinal | Logical retry and conflict reconciliation |
| Content fingerprint | Original Phase 13 SHA-256 of Python `repr((seconds, action, (str(peer ASN), peer IP), prefix address, length, path ID, attribute hash bytes))` | Description only; **never deduplication** |

Policy binding is SHA-256 using the existing typed namespace and components
`mrtparse-2.2.0`, `ris-wire-v1`, `feasibility-v1`. `ris-wire-v1` names the
wire-section expansion/canonical prefix policy; it does **not** claim the
attribute digest is a hash of raw wire bytes. Attribute hash is the original
`sha256(json.dumps(parsed_path_attributes, sort_keys=True).encode())`.
Fingerprint repetition counts occurrences **beyond the first**, not the number
of repeated keys. Equal payload/time at another offset or ordinal remains an event.

All-record indices include excluded/unsupported frames and reset per artifact.
They are audited but not substituted for byte offsets in the identity. Ingestion
time, Kafka offsets, query UUID and processing time are not source identity.
Source times are recorded seconds or extended microseconds, not verified packet
arrival accuracy or operator-action timestamps. Ties/disorder are preserved.

## Expansion and support

Order: conventional withdrawals in wire order; conventional announcements in
wire order; MP attributes in wire order, each prefix in that attribute's wire
order. Ordinals start at zero per source record. Supported event subset is
BGP4MP/BGP4MP_ET unicast UPDATE, IPv4/IPv6 MP reach/unreach, and the corresponding
ADD-PATH message subtypes. Optional path ID is preserved. Non-UPDATE/control and
zero-NLRI records remain in framing accounting but are not fabricated events.
Unsupported family/SAFI or invalid accepted projections fail the run; well-framed
parser failures remain explicit exclusions, never byte resynchronization.

Framing follows [RFC 6396](https://www.rfc-editor.org/rfc/rfc6396.html);
MP and ADD-PATH layouts follow
[RFC 4760](https://www.rfc-editor.org/rfc/rfc4760.html) and
[RFC 8050](https://www.rfc-editor.org/rfc/rfc8050.html).
Record bodies are bounded to 1 MiB, expanded production records to 4,096 prefixes.
Normal tests use previously project-authored Phase 16 binary vectors only.

## Oracle independence and limitations

1. A separately authored gzip/header loop compares all record offsets, indices,
   timestamps, type/subtype and bodies against the production framer, through EOF/CRC.
2. Literal owned conventional, MP, ADD-PATH and extended-time vectors constrain
   expected prefix lists; malformed/unsupported frames precede valid frames.
3. `workloads/ris_reference.py` independently decodes peer context, UPDATE length,
   conventional/MP NLRI, path IDs and source time. Its identity implementation
   does not call production canonicalization/hash helpers.
4. Private complete event projections and full source-coordinate ledgers support
   exact comparisons, not just counts. Public artifacts contain aggregate/digest
   summaries, not externally sourced prefixes, peers or AS paths.

**Qualification:** attribute JSON decoding/digests and the original descriptive
fingerprint still depend on pinned mrtparse. The independent extractor validates
NLRI/coordinate/identity/peer/time projection, not every AS_PATH/attribute semantic.
The Parquet oracle is therefore not advertised as a fully independent oracle of
every field. Full-hour independent occurrence/projection acceptance remains open.
Counts matching the old survey cannot close that gate.

## Exclusions and evidence boundary

All twelve artifacts frame independently: **2,300,578 records**, **2,291,545
accepted UPDATE messages**, **57 pinned-parser body errors**, **427,068,595
decompressed bytes**. Each exclusion is type 16/subtype 4, fully framed, with
`Insufficient buffer 0 < 1 byte`. The minimal NLRI extractor sees zero prefixes
in all 57. This does not certify complete BGP validity or authorize changing the
old exclusion policy. No excluded frame becomes an accepted event. Subsequent
coordinates match independently; private complete coordinate ledger checksum is
in [framing evidence](../workloads/results/phase18/framing-validation.json).

The complete first archive's layered oracle observes 370,141 occurrences,
187,480 frames, five exclusions, 98 peers and 64,607 prefixes. The remaining
archives have full framing/error accounting but **not full occurrence-oracle or
service acceptance**. Historical 4,725,156 occurrences / 118,172 repeated
fingerprints remain Phase 13 references, not new Phase 18 outcomes.

Raw hash/policy mismatch, source-coordinate conflict, unexpected/missing accepted
identity, checksum corruption or invalid transport stops acceptance. Recovery
and invariant evidence levels are in [recovery](phase18-ris-recovery.md) and
[acceptance](phase18-acceptance.md). None of this reconstructs a RIB or identifies
configuration edits, routing incidents or authoritative causes.
