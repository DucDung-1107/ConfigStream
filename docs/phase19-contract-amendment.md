# P19-DNS-1.1 — 2026-10-14

Entry: **GO — PHASE 18.2 ACCEPTED**, at clean main
`eaa8fdaaa16ade4a5fe21beb40a033c31c9cebae`; its
[CI passed](https://github.com/kongwoang/ConfigStream/actions/runs/38428875500).
RIS recovery established the complete 4,725,156-row logical projection and
published 57 exclusions plus twelve source descriptors. Previous failures remain
recorded. Instructor and unrestricted external redistribution approvals are not
assumed. The owner separately authorized reasonable resource use without further
questions; no infrastructure expansion is planned.

Phase 16 DNS payload 1.0 represents A/AAAA observation fixtures. Real selected
objects contain 99 columns and all DNS result types, including eighteen successful
AFSDB-query/A-response rows per day in the frozen address projection. Mapping all
rows into that narrow payload would lose evidence or change the counting policy.

Add explicit DNS-only payload **1.1** and a typed physical source-row projection.
Reuse the common envelope fields, ParquetCoordinate, policy-bound identity and
source manifest validation. Original Envelope/Evidence/payload 1.0 dispatch remains
strict and unchanged; an explicit DNS parser selects 1.1. No record is coerced
into ConfigSnapshot, RPKI state or RIS occurrence. Tests reject legacy dispatch
of 1.1 and unknown versions, incorrect coordinates and fabricated precision.

Raw projection preserves original query/response/time/worker/status/context fields,
canonical address evidence where eligible, and the SHA of the complete original
99-field row. Original Parquet stays private and is retained once for full record
reproduction. RTT uses exact binary64 hexadecimal encoding in the projection,
avoiding loss of source bits through decimal equality. Source identity remains
artifact/row-group/original-row/policy based, never value based.

Frozen address policy remains status 0, nonempty A/AAAA response value, four-field
key with lowercased/dot-trimmed names, canonical family-valid IP. Query type is not
silently restricted to A/AAAA. Group/value/contributor identities have distinct
roles. All real entity/answer coverage and resolver/vantage knowledge remain unknown.

Verified dictionary: 65535 is timeout. The selected files instead contain 65533,
whose meaning is not established by that dictionary; retain the numeric code and
classify it unknown. RCODE 2/3 are unsuccessful/negative evidence, not deletion.
No real `proven_empty` classification is authorized by these RR-level objects.

Source scope clarification: exactly two selected published objects, not every
daily partition shard, the whole `.li` population or an authoritative zone.
Phase 13 numbers and historical wording are not rewritten. Strict matched-vantage
eligibility remains unproven; recorded-worker sensitivity is a separate analysis.

Sources: [dictionary](https://openintel.nl/data/dictionary/),
[terms](https://openintel.nl/download/terms/). See [source acknowledgment](phase19-source-attribution.md).

## P19-WINDOW-1.0 — additive final acceptance metadata

Raw/source/analytical/comparison commit versions remain unchanged. Add an immutable
`dns-selected-window-1.0` descriptor binding the whole raw inventory, both source
completion hashes, complete source-reference wrapper hashes and comparison hash.
It detects corrupted selection-date metadata outside the underlying SourceManifest
hash, as well as unexpected artifact scopes. Readers verify the metadata chain;
physical table readers additionally verify content. Existing source-only commits
do not implicitly acquire this stronger window guarantee.

The accepted two-object run completed source/comparison publication at `1d5e388`.
A separate bounded 256-row replay plus complete committed group/value readback at
`3ef6f6c` published the additive window descriptor. Original raw batches, source
descriptors and comparison metadata were not overwritten or retrospectively relabeled.

The final time audit aligns group unknown-time flags with the existing raw
KnownTime/UnknownTime supported epoch range. Invalid encoded times remain in raw
evidence but do not become a known group time range. Owned negative-time tests
cover this. All selected real timestamps are nonnull and inside that range, as
verified by the complete reference minima/maxima; the frozen group projections
and identities therefore remain unchanged. No source replay is required.
