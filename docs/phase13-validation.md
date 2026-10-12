# Phase 13 Round 1 validation record

The work adds bounded data-inspection scripts, offline tests, documents and
compact summaries. It does not change schemas, streaming state, lake commit
semantics, analytics jobs, search projection or API behavior.

## Local acceptance

On 2026-10-12:

| Command | Result | Scope |
|---|---:|---|
| `make feasibility-test` / matching pytest files | 34 passed | New offline fixture tests |
| `make check` | 823 passed, lint/format passed | Existing 789 plus 34 new unit cases |
| `make api-test` | 80 passed | Service-free API; one dependency deprecation warning |
| `make spark-test` | 60 passed | Existing service-free Spark transformations |

The initial implementation's [CI run 38190403738](https://github.com/kongwoang/ConfigStream/actions/runs/38190403738)
passed both jobs (822 unit, 80 API, 60 Spark). A subsequent command-reproduction
check found and corrected a missing DNS partition-date field; an additional
offline regression test increases local unit coverage to 823. This record
distinguishes the verified initial CI from the follow-up local correction;
consult the latest pushed workflow for the follow-up commit's status.
Machine-readable validation is in
`workloads/results/dataset-feasibility/validation.json`.
Service integration suites were **not rerun**: no pipeline/service code
changed and Round 1 explicitly prohibits new integration. Prior Phase 12
service results remain in its immutable acceptance record; they are not
claimed as new Phase 13 executions.

Tests cover allowlisted manifest downloads, checksums, archive CRC, CSV
schemas/delimiters/headerless payload suppression, canonical RPKI keys,
timestamps, gaps, unchanged states, duplicates, MRT event expansion and sticky
parser-error recovery, DNS name/IP canonicalization, registry completeness,
score bounds, overlap and deterministic report generation. CI uses no live
requests, downloaded corpora or optional MRT/DNS libraries.

The real probe record is separate: 14 RPKI files, 12 MRT files, two DNS
partitions, 15 small research artifact files, bounded fix inspection and two
short ZoneStream sessions. Expected blockers and corrected failures remain
in `probe-audit.json`; successful summaries do not erase them.

Reproducible SVGs are generated from measurements, not manually entered chart
values. N/A/unverified access is not converted into zero events or fabricated
PeeringDB time series. The recommendation is a future scope decision, not
a declaration of implemented multi-domain pipeline support.
