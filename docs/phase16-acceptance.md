# Phase 16 — acceptance record

**CONDITIONAL PASS — OPEN GATES REMAIN.** Stop before Phase 17.

Starting clean `main`: `362fb7cbee02258456ca7ed3e9042b6d720ae8bd`.
This acceptance covers reversible W0–W3 foundations and RQ1 offline evidence,
not a real new-domain pipeline or completed K1/K2/K3 research contribution.
See [implementation](phase16-implementation.md), [identities/oracles](phase16-identity-and-oracles.md),
[controlled amendment](phase16-dependency-amendment.md) and
[machine validation](../workloads/results/phase16/validation.json).

## Test results and honest coverage

Verified starting `make check`: 823 passed in 13.55 s. Final local suites:

| Command | Result | Baseline / added |
|---|---:|---:|
| `make check` | **909 passed**, 13.44 s; lint/format pass | 823 / +86 |
| `make api-test` | **80 passed**, 0.59 s; one existing warning | 80 / +0 |
| `make spark-test` | **63 passed**, 51.06 s | 60 / +3 |

These are 1,052 service-free cases, including 89 added cases, not new external
integration acceptance. Exact logs stay ignored under `.cache/phase16/`.
86 new offline cases cover contracts, identity, provenance, timing, state barriers,
DNS uncertainty, framing/expansion, publication and backward compatibility.
Three new service-free local Spark/Parquet cases test typed and legacy recovery.
These are **not MinIO/Kafka integration**.

The initial targeted Spark attempt accidentally used lake-enabled session settings,
triggering connector resolution; it was interrupted before tests ran (155.57 s).
The test now creates a pure local session with no connectors. Corrected targeted
run: **3 passed in 9.92 s**. Failed/corrected attempts remain in ignored logs and
the validation record. An old API Starlette/httpx warning is not a new regression.

No Docker executable was available on PATH or checked standard paths. No services
were launched; actual MinIO conditional publication, actual checkpoint interruption
and existing external-service integration suites are **PENDING**, not passed.
Local Parquet is genuinely written/read; conditional manifest persistence uses
owned in-memory stubs. The `IfNoneMatch=*` request is tested, not service races.

## I1–I9 verification matrix

| Invariant | Status | Actual evidence | Still pending |
|---|---|---|---|
| I1 provenance | PASS_OFFLINE | Hash/size verifier, manifest/artifact references, invalid binding rejection | Real acquisition/oracle trust across whole sources |
| I2 replay | PASS_OFFLINE | Same source ID retries collapse, conflicting payload/commit fails | Persistent services/restart over complete input |
| I3 multiplicity | PASS_OFFLINE | Equal payload/time at distinct MRT/Parquet coordinates survives | Full RIS expansion/occurrence ledger |
| I4 completeness | PASS_OFFLINE | Complete-state member proof; partial/gap/regime/checksum rejection; DNS absence cannot be a row | Real day/partition completion and independent oracles |
| I5 temporal honesty | PASS_OFFLINE | UTC/original precision, unit mismatch, unknown, ties and inversions | Full source timestamp ledger |
| I6 visibility | PARTIAL | Failed output unpublished; real local Parquet reads/checksums; commit-before-ack retry simulations | Actual MinIO conditional-put and Spark checkpoint interruption |
| I7 reconstruction | PASS_OFFLINE | Literal member/checksum/delta and qualified DNS fixtures | Real historical reconstruction, not zones/routes |
| I8 accounting | PASS_OFFLINE | All-frame indices, fatal truncation, safe error ledger, parser exception audit | Full archive EOF/integrity, original 57-error ledger |
| I9 compatibility | PARTIAL | Old critical-file hashes; unit/API/Spark regressions; legacy manifests/snapshots serialize/read | External-service suites and actual old checkpoint restart |

PASS_OFFLINE does not mean full domain correctness is proven. Detailed named tests:
[invariant coverage](../workloads/results/phase16/invariant-coverage.json).

## Mandatory positive/negative matrix

| Area | Positive | Negative / failure policy |
|---|---|---|
| Envelope | Valid closed kind/version | Unknown kind/version/extra fields reject |
| Identity | Independent literal vectors/NFC/stable scalar tags | Same-ID conflicting payload rejects |
| Provenance | Pinned artifact/manifest and known SHA256 fixture | Wrong/missing reference/hash/size fails |
| Time | Offset/precision/unknown/arrival preserved | Wrong units/naive/invented timestamp fails |
| RPKI | Complete consecutive sets and exact reconstruction | Partial/gap/regime/accounting mismatch forbids edge |
| DNS | Qualified nonempty/proven-empty observed sets | Timeout/missing cannot become deletion or fake raw row |
| MRT | Literal offsets, MP/ADD-PATH, isolated parser | Truncation fails; unsupported/malformed body ledger retains coordinate |
| Multiplicity | Equal events at different offsets survive | Payload-only dedup would lose expected fixture occurrence |
| Replay | Same coordinate retry is one logical record | Conflicting coordinate payload/publication fails |
| Lake | Registered committed paths with actual read checksum | Partial/uncommitted output invisible; wrong path/output fails |
| Compatibility | Old ConfigSnapshot/allowlist/manifests unchanged | New Evidence cannot serialize as old config transport |
| Roadmap | Full and reduced DAG closure | Mandatory W9 in reduced graph is a dangling dependency |

## RIS hard gate decision

**RIS_OFFSET_IDENTITY_PROVEN** on owned fixtures and cached first 1,000 records,
160,268 decompressed bytes. All 1,000 prefix records parsed; no framing drift;
zero source downloads. Full gzip SHA256 matches original Phase 13 metadata.
Pinned parser unknown-type `KeyError` is explicitly categorized by independent
frame isolation. Full-hour 57 exclusions/118,172 repeated fingerprints remain
unchanged and un-replayed. Full-hour/session/parser coverage is a Phase 18 gate.

## Resources, legal safety and approval

Host M3 Pro/12 cores/18 GiB; available disk remains above 20 GiB reserve.
Bounded proof process max RSS **37,191,680 bytes**, measured wrapper wall time
0.25 s; not aggregate Spark RSS or ingestion throughput. New probe cache observed
216 KiB; local Spark tests use a 1g driver setting, not a measured total-memory
guarantee. No new source downloads, corpus remine, benchmark, cloud, production
topics/checkpoints/data or raw external Git content. Optional mrtparse 2.2.0 is
isolated in ignored cache; base/production dependencies are unchanged.

Owned binary/JSON fixtures use documentation addresses and `.invalid` names.
The cached real prefix contributes only safe coordinates/counts/hashes to Git.
Instructor approval, team-hour commitment and external raw-publication rights are
not evidenced. Public URLs are not redistribution approval. Source-use/attribution
checks and actual container allocation must close before expensive source work.

## P16 gates and Phase 17 prerequisites

A research freeze and H dependency closure PASS. B contracts, C identities,
D offsets and E completeness PASS_OFFLINE. F publication remains CONDITIONAL;
G compatibility passes offline with service scope pending; I is CONDITIONAL for
external approval/legal/resource allocations. See
[acceptance.json](../workloads/results/phase16/acceptance.json).

Before Phase 17: separate authorization/instructor and budget evidence; bounded
MinIO/recovery and relevant legacy service checks; verified use/attribution for
the exact RPKI window; independent day oracle and scalable member accounting;
host/container safety limits. Do not simply raise offline 10,000-row guards into a
full-source processor. No real new-domain pipeline is presently wired.

DNS-specific legal/qualification/resource failure selects Track R, not a weakened
four-role claim. Identity/completeness/old-reader correctness failures stop affected
implementation; unresolved common publication correctness stops all source work.
Rollback is reverting unwired additive modules/targets: no old history migration,
schema rewrite or checkpoint deletion is necessary. Do not delete user volumes.

CI/commit are recorded after push verification; do not infer success from local tests.
