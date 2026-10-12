# Phase 16 — additive temporal foundations

Starting SHA: `362fb7cbee02258456ca7ed3e9042b6d720ae8bd`, clean `main`.
Verified initial `make check`: **823 passed in 13.55 s**. Phase 15's recorded
API/Spark baseline is 80/60. This phase implements W0–W3 only; planned effort
30/52/96 person-hours is not measured development time. No Phase 17 adapter.

## Controlled decision first

[P16-2026-10-12-DNS-OPTIONAL](phase16-dependency-amendment.md) was written before
production code. Track F retains Terraform/RPKI/RIS/DNS and E4; Track R retains
Terraform/RPKI/RIS with mandatory recovery and baselines. Only F can test four-role
K3. Original Phase 15 roadmap and all empirical results remain unchanged.

## Implemented boundaries

| Module | Purpose / Phase 15 binding | Deliberate limit |
|---|---|---|
| `schemas/temporal/base.py`, `time.py`, `coordinates.py` | Frozen bounded contracts; required tagged time/coordinate; unknown remains unknown | No universal entity/version or inferred event time |
| `identity.py` | Typed canonical identity namespace and audit preimages | Not a distributed collision registry |
| `provenance.py` | Immutable artifact/policy/selection/terms/accounting manifest and streamed size/hash verification | `integrity_verified` is a declaration requiring acquisition/oracle proof, not a constructor proving external bytes |
| `records.py`, `errors.py` | Closed envelope/payload validation; typed RPKI descriptor/transition, DNS measurement, RIS occurrence; separate frame error ledger | No coercion into old ConfigSnapshot; no payload-bearing diagnostic |
| `reconciliation.py` | Exact same-ID/evidence retry collapse; conflicting evidence fails | Bounded 10,000 deliveries per call; not persistent watermark dedup or global transaction |
| `processing/temporal_state.py` | Small offline completeness, set checksum/reconstruction and qualified DNS-difference tests | Owned-state fixture bound; **not** full-corpus or Spark transition processing |
| `processing/mrt_framing.py` | Independent streaming RFC 6396 record coordinates, explicit fatal framing errors | Framing only, no BGP parser, importer or route reconstruction |
| `ris_expansion.py` | Validated wire-section expansion order for owned/pinned-parser projections | No binary decoding; MP/ADD-PATH parser coverage still gated |
| `lake/temporal.py` | Explicit immutable registry; isolated conditional commit metadata; opt-in publication and exact logical read reconciliation | Trusted output/reader callbacks; no new Spark job, production topic, deployment or automatic registry dispatch |
| `scripts/phase16_coordinate_probe.py` | Explicit cached-only bounded parser/framer cross-check | Not CI/live ingestion; optional parser isolated in ignored cache |

The envelope has exactly the Phase 15 eleven common fields, including explicit
`1.0` versions. Domain payload fields stay outside it. Unknown kinds/versions and
extra fields fail closed. Model-copy bypasses are not trusted: reconciliation
revalidates serialized records, and manifest binding revalidates manifests.

## Typed publication and reading

The closed registry reserves `temporal/rpki/observations`, `temporal/rpki/transitions`,
`temporal/dns/measurements`, `temporal/ris/occurrences` for schema 1.0. None are
created in the production lake by this phase. Configuration remains outside this
registry. Its original schema map, writers/readers and `_commits` format 1.0 stay
unchanged. New metadata uses `_temporal_commits/v1/<query>/<batch>/<kind>.json`
and format `temporal-1.0`, so original commit enumeration cannot consume it.

An explicit caller validates/reconciles source IDs, then writes the registered
query UUID/batch path, then conditionally publishes metadata (`IfNoneMatch=*`).
The trusted callback must overwrite only uncommitted output and fail on partial
writes; Phase 16 tests JSON and actual local Parquet callbacks. Already committed
batches compare full planned logical projection hashes, kind, path and provenance,
not counts alone. Retry disagreement fails without overwriting committed output.
Readers accept frozen committed manifests, never glob uncommitted files; the
logical projection reader validates actual loaded ID/payload checksums/counts.
Selected repeated runs reconcile source IDs rather than payloads.

RPKI publication additionally requires complete member proofs; transitions require
both input state proofs, exact membership differences and bound artifact lineage.
A file-batch commit is **not** a daily state-completion barrier. Descriptor reads
alone do not reconstruct RPKI membership: `complete_rpki_state` independently checks
the supplied members. Incomplete drafts remain pending, not published states.

There is no multiwriter arbitration, global source-ID database, Kafka/MinIO/Spark
checkpoint atomic transaction, automatic completion discovery or state migration.
The service-free tests cannot prove MinIO concurrency/durability or an actual
checkpoint interruption; these remain service gates.

## Compatibility and exclusions

Existing schemas, serializers/topics, version buffers, normalization/rules,
Spark parser/checkpoints, lake schemas/paths/manifests, analytics and search/API
are untouched. Terraform's existing fixture loader and serialization are audited;
no snapshot wrapper, migration or changed ordinal was introduced.

No real RPKI/DNS ingestion, full MRT publisher/replay, new topics/jobs/API/rules,
new dependencies in `pyproject.toml`/`uv.lock`, corpus remine or benchmark occurs.
No external raw bytes are committed. Owned `.invalid`/documentation-address
fixtures are not evidence of natural activity. Research RQs/K1–K3 remain prospective;
this prepares RQ1 only, not RQ2/RQ3 cost results.

Reproduce: `make phase16-test`, `make check`, `make api-test`, `make spark-test`.
Cached-only proof requires the separately pinned `mrtparse==2.2.0` in
`.cache/phase16/parser`; `make phase16-coordinate-proof` performs no download and
writes to ignored cache. [Identity/oracles](phase16-identity-and-oracles.md) and
[acceptance](phase16-acceptance.md) distinguish actual tests from pending gates.
