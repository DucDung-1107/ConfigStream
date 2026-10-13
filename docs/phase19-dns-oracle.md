# Phase 19 — Independent DNS reference

`workloads/dns_oracle.py` uses PyArrow original row-group enumeration and SQLite,
not the production importer normalization or Spark transformations. It independently
authors typed identity preimages, IP-family validation, name canonicalization,
status disposition, full raw-row projection, groups, values and contributor links.
Shared schemas declare the contract, not expected transformed answers.

Production uses bounded `iter_batches`; reference uses original `read_row_group`
followed by 4,096-row slices. The two cached objects each contain one row group.
The reference's native row-group allocation is measured, not constant-memory for
arbitrary future source layouts. SQLite stores exact row-ID/projection mappings
and group contributors on disk. Per-group evidence is bounded to 4,096 rows.

All 2,234,863 original coordinates are enumerated. Hash/size/footer counts are
verified before publication. Canonical sorted JSON with newline separators produces
the raw identity-order digest, source-coordinate-order digest and group digest.
Exact full-field comparison, not digests alone, validates Spark output. Contributor
links are independently compared in both directions with multiplicity. A separate
SQL common-group ledger calculates exact before/after/added/removed/unchanged
arrays and worker sensitivity; Spark compares the entire ledger by group key.

| Date | Raw rows | Selected addresses | Groups | Distinct query names |
|---|---:|---:|---:|---:|
| 2026-09-01 | 1,117,660 | 176,790 | 145,559 | 172,474 |
| 2026-09-02 | 1,117,203 | 176,690 | 145,368 | 172,318 |

The independent source pass reproduced the frozen descriptive reference:
144,883 common groups; 4,091 differing observed sets; 7,716 added / 7,612 removed /
168,436 unchanged observed values; 485 newly / 676 no-longer-observed groups.
Recorded-worker sets match for 55,517 common groups; strict matched-vantage
eligibility is zero and qualification remains unknown for all 144,883.

Private outputs contain full source/group/value/contributor ledgers and SQLite
records. Public summaries contain only approved counts, policy bindings and hashes.
No normalized names or addresses are committed. Original source artifacts and
their exact hashes remain pinned; no new objects or counting-policy changes.

Reproduce on cached, authorized inputs:

```sh
make setup
.venv/bin/python -m workloads.dns_oracle --output .cache/phase19/oracle-new --max-seconds 1200
```

Use a fresh output path. Do not overwrite existing evidence. This reference is a
simple batch correctness baseline, but its integrity, SQLite and independent
validation work is not a matched throughput baseline for Kafka/Spark. Full matched
three-repetition evaluation remains Phase 20, which has not been started.
