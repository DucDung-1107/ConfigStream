# Phase 19 acceptance

**PASS — FULL DNS MEASUREMENT SEMANTICS AND E4 VALIDATED**

This decision covers exactly the two selected OpenINTEL `.li` zonefile-basis
objects, not complete DNS entity coverage or authoritative zone history. All
descriptive changes retain unknown causality. Strict matched-vantage eligibility
is zero; this restriction is preserved, not waived to obtain acceptance.

## Engineering evidence

| Requirement | Result and scope |
|---|---|
| Entry | GO; Phase 18.2 full-hour RIS retained-history recovery accepted at `eaa8fdaaa16ade4a5fe21beb40a033c31c9cebae` |
| Source integrity | Two pinned hashes verified; 99 fields and one original row group per object; no new download |
| Raw coordinates | Exact 1,117,660 / 1,117,203 original rows; all 2,234,863 preserved, independently compared; no missing/extra/conflicting logical projection |
| Status | 0 successful; 2/3 unsuccessful; 65533 explicitly unknown; real 65535 count zero, timeout owned fixtures covered |
| Time and workers | Original epoch-ms integers, second precision, worker IDs and group ranges preserved; four recorded workers per source, not verified resolvers |
| Address projection | Exact 176,790 / 176,690 selected rows and 145,559 / 145,368 groups; includes frozen AFSDB/A cases |
| Independent oracle | Separate PyArrow/SQLite normalization and identity/group/value/contributor maps; full exact comparisons, not count-only acceptance |
| Descriptive comparison | Exact 144,883 common groups, 4,091 differing sets, 7,716 added / 7,612 removed / 168,436 unchanged values |
| Availability | 485 newly / 676 no-longer-observed successful address groups; no entity-deletion inference |
| Strong qualification | 0 matched-vantage eligible; 144,883 common unknown-qualified; 1,161 unmatched address groups ineligible; same recorded-worker-set sensitivity 55,517 |
| Reuse/reconstruction | Full source/group/contributor projection exact; remote reused-value join reconstructs all fields; no erased measurement occurrences |
| Service run | Both complete objects passed Kafka → Spark → MinIO at `1d5e38858abee18b740fc4bad462c81f9f935fde` |
| Broker | 17,461 chunks; 2,234,863 acknowledged rows; 1,994,776,495 serialized value bytes; final lag zero |
| Spark | 70 callbacks, 485.622 s combined callback work; no full-driver row collection or OOM |
| Source finalization | 121.114 / 120.174 s; exact barriers precede immutable analytical/source/comparison metadata |
| Recovery | 256 real source rows replayed after completion; zero added logical rows; same checkpoint advanced, immutable raw inventory unchanged, lag zero |
| Durable window | Additive selected-window descriptor published during bounded replay/readback at `3ef6f6cb56e1162435f1af4af284de9b2cb86af4`; complete source references and comparison are bound without rewriting original commits |
| Fault injection | 14 actual owned-fixture cases: four Kafka checkpoint cases plus ten Spark/MinIO integrity cases; no power-loss/failover claim |
| Queries | Exact source/group maps, contributor links, worker/time qualifications and full common-group difference ledger; complete remote reuse reconstruction; private names never published |
| Resources | 1,224.086 s within 3,600-s cap; peak sampled process-tree RSS 2,742,534,144 bytes; guest disk minimum 32,315,916,288 bytes, available RAM minimum 4,382,691,328 bytes; guard failures none |
| Physical storage | 538,543,164 Parquet bytes, 160 files; after additive window metadata 539,562,365 object-content bytes; checkpoint 64,079 bytes after replay |
| Reuse cost | Full components 448,045,122 bytes versus reused components 450,252,590 bytes: **reuse is 2,207,468 bytes larger**, not a storage saving |
| Batch baseline | Independent same-projection source path completed in 302.21 s, process RSS 1,048,477,696 bytes; includes different integrity/SQLite/validation work, not a matched speed comparison |
| Regression | 1,079 unit/quality, 80 API and 107 Spark tests passed; existing Terraform/RPKI/RIS contracts unchanged |
| Rights | Verified CC BY-NC-SA 4.0 private noncommercial research use and required acknowledgment; no external rows published; instructor approval NOT_VERIFIED |

The first sample's offset-form failure and the initial destroyed Kafka-admin future
failure remain private negative evidence; fixes have focused tests. Successful
source runs are not relabeled versions of failed attempts. No lengthy retry,
infrastructure expansion, volume reset or deletion of old evidence occurred.

## I1–I9

| Invariant | Evidence |
|---|---|
| I1 provenance | PASS_REAL_SOURCE + PASS_INTEGRATION; original artifact/row-group/row and immutable reference bindings |
| I2 replay | PASS_INTEGRATION; exact real-source 256-row no-op and owned checkpoint retries |
| I3 multiplicity | PASS_REAL_SOURCE + PASS_OFFLINE; complete raw maps and contributors, equal values do not merge source rows |
| I4 completeness | PASS_REAL_SOURCE + PASS_OFFLINE + PASS_INTEGRATION; source/group barriers and uncertainty counterexamples; entity coverage remains unknown |
| I5 time | PASS_REAL_SOURCE; source clock/encoding/precision preserved; transport/processing clocks qualified separately |
| I6 visibility | PASS_INTEGRATION; missing/corrupt source/data/group/value/window evidence rejected in owned prefixes |
| I7 reproduction | PASS_REAL_SOURCE + PASS_INTEGRATION; full exact maps and remote value/contributor reconstruction |
| I8 accounting | PASS_REAL_SOURCE; successful, non-target, failed and unknown rows all retained; no real empty inference |
| I9 compatibility | PASS_OFFLINE + PASS_LOCAL; unit/API/Spark regression; old experiment namespaces unchanged |

## Limits and next-phase gate

RQ1 and E4 are supported for the selected scope. K3 is a bounded four-role
demonstration, not universal generalization. RQ2/RQ3 remain preliminary; no
streaming superiority, DNS change-rate or matched performance claim is made.
The prototype jointly retains full and reused group representations to validate
them; component byte comparisons are not a standalone reduced-retention deployment.
Original source files are required for nonprojected 99-field source reconstruction.

Kafka CreateTime and checkpoint batch initiation clocks are separate from source
time. Exported Kafka clocks are offset-endpoint samples, not a complete individual
delivery-receipt ledger; actual network arrival/latency and clock accuracy are not
established. Callback faults do not prove power-loss durability or multiwriter ACID.

Full Track F is **technically eligible for a separately requested Phase 20 plan**;
instructor approval, source-publication obligations and new matched experiment
budgets remain separate gates. Track R remains an available fallback, not currently
required by a DNS technical failure. **Phase 20/21 has not been started.**

## Reproducibility and report links

The starting SHA and execution SHAs above are frozen. CI on implementation commit
`3ef6f6cb56e1162435f1af4af284de9b2cb86af4`
[passed](https://github.com/kongwoang/ConfigStream/actions/runs/38445187165).
Final evidence-commit CI is verified separately on its exact pushed SHA; it must
not be represented as testing live OpenINTEL/Kafka/MinIO in GitHub Actions.

See [architecture](phase19-dns-architecture.md), [uncertainty](phase19-dns-uncertainty.md),
[oracle](phase19-dns-oracle.md), [experiments](phase19-dns-experiments.md),
[recovery](phase19-dns-recovery.md), [controlled amendment](phase19-contract-amendment.md),
[attribution](phase19-source-attribution.md), [validation](../workloads/results/phase19/validation.json),
[oracle/source schemas and digests](../workloads/results/phase19/independent-oracle.json),
[representation costs](../workloads/results/phase19/representation-costs.json) and
[delivery clocks](../workloads/results/phase19/delivery-time-qualification.json).
