# External validity and project framing

## Phase 12 decision: B

[TerraDS](terrads.md) is materially larger static evidence: 1.51 million public
Terraform files and 2.60 GB source. The fixed 500-origin sample actually observed
10188 accepted revisions across 6505 incarnations in 375 repos. The weighted
recoverable-history estimate is ~1.68 million states (95% bootstrap ~1.31–2.14
million), **not a mined temporal dataset**. Only ~609 thousand adjacent comparisons
are estimated; states include initial baselines. The filtered byte estimate is
~2.03 GB, not terabytes of natural temporal telemetry.

Accepted Terraform history is richer than FRR/NGINX, but 77.48% of incarnations have
one version. Median uncensored spacing is 6.71 days among the changing subset,
not apply/deployment cadence. License evidence, conservative scanner false
positives, examples/forks/vendor code, omitted branches and deleted origins limit
generalization. A 484-file bundle includes introductions; the maximum correlated
existing-file edit bundle is 39. Neither proves production rollout behavior.

The experiment strengthens realism and validates a generic pipeline, not a claim
that configuration change naturally demands high-rate streaming. Keep the project
as an **empirically grounded Big Data architecture evaluation for infrastructure
configuration history**, subject to instructor agreement. If the course requires
inherently massive *observed temporal* data, changing topic is safer than further
data-search phases. [Full decision and uncertainties](phase12-acceptance.md).

**Git history is an observable public proxy for configuration evolution.**
It does not directly measure router configuration-change/deployment frequency.

## Three evidence layers

1. Truly observed: accepted file bytes actually existed at selected pinned FRRouting
   and NGINX Git commits. File transitions, sizes, timestamps, exact repeated hashes,
   and multi-file commit bundles are measured from those objects.
2. Controlled/semi-synthetic: the Phase 10 Batfish-derived histories, and Phase 11
   `observed-calibrated` whole-history resampling. Their generated asset IDs, sample
   sizes/multipliers and replay rates are artificial even when source bytes are real.
3. Fully synthetic: original generator templates. `stress-replay` is a capacity
   workload label, not evidence of naturally high per-device frequency.

The million-snapshot Phase 10 result remains semi-synthetic. Ten thousand cloned
assets are not ten thousand observed devices. Phase 11 observes **1906 file-incarnation
assets / 2525 revisions / 1,133,998 content bytes**; calibration creates **76,240
cloned assets / 100,518 revisions**, not an observed enterprise dataset.

## Measurement biases

- Public test configurations/templates differ from private enterprise running configs.
- A Git commit can bundle many logical edits or template changes; it need not correspond
  to any deployment. Commit time is not deployment time; author/committer time can differ.
- First-parent merges hide branch-internal changes; this is not all DAG history.
- Initial baselines are left-censored. Inter-change intervals omit intervals whose
  previous version is a boundary baseline. Small samples (NGINX has one uncensored
  interval) cannot support stable tail estimates or broad frequency claims.
- Renames are not followed; deletion/recreation splits assets. This loses continuity.
- Scope, UTF8/size and whole-path secret filtering bias accepted populations. Very
  small/comment-only public config files remain represented rather than padded.
- Zero-change calendar bins are Git observation bins, with partial first/last bins;
  they are not measurements of idle deployed routers.
- Text normalization/diff is not vendor-semantic impact validation. `changed=0` can
  accompany additions/removals; alerts are regex rule matches, not incidents.
- Exact hash returns are **content reversions**, not automatically operational rollback
  or failure recovery. Multi-file changes are correlated edits, not proven fleet rollouts.
- Bootstrap cloning greatly lowers unique-content ratio and can improve storage reuse.
  Raw+processed Parquet size includes schema/file overhead and is not raw-only compression.
- Local benchmarks use one host, different data shapes and replay-specific watermark/
  state settings; they do not prove production/horizontal scaling or exactly-once delivery.

## What generalizes?

Safe project claims: deterministic extraction/replay, version-aware native state
correctness, committed-history analytics, explicit serving security and measured local
behavior for these workloads. Observed selection statistics generalize only to the
specified public scopes/revisions/filter policy. Hypotheses about fleet×versions×retention
are motivations to test elsewhere, not verified industry-volume statistics.

Do not claim production device counts, deployment intervals, secret-detection completeness,
incident accuracy, 1000 natural changes/s/device or production compression/cluster capacity.
No industry per-router change-rate estimate is invented.

## Does the empirical evidence support the Big Data framing?

**Weak natural-volume evidence.** The selected observed history is about 1.13 MB,
median versions/asset is 1, and the combined uncensored median inter-change interval
is about 126 days. The combined median snapshot is 298 bytes, smaller than the
Phase 10 Batfish baseline median 557.5 bytes. Correlated edits do occur (up to 136
selected files/hour), but they are mostly public test/template evolution, not evidence
of enterprise fleet activity.

Phase 11 strengthens *workload validation* while weakening any unqualified claim
that this is naturally high-frequency configuration telemetry. Fleet aggregation,
long retention and correlated deployments could motivate scale, but these sources
do not establish their production volume. Position the project as **a Big Data
architecture evaluation using configuration history as an empirically characterized
workload**, not as proof of naturally massive configuration telemetry.

Recommendation: **keep the project, narrow/reframe its empirical claims**. Do not
rename or abandon working architecture automatically. Obtain an ethically shareable,
larger observed operational corpus before making stronger natural-volume claims;
alternatively defend the reproducible pipeline/state/storage evaluation itself.
No dashboard, deployment, live collection or ML is implemented in this phase.
