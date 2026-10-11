# Phase 12 methodology: real IaC population and recoverable Git history

This experiment is single-agent, local, and data-only. It preserves the existing
Kafka/Spark/MinIO/search/API architecture and existing rules. External HCL is never
executed: no Terraform init/plan/apply, external Compose, Ansible, Helm or kubectl.

## Four different evidence units

1. **Full static corpus**: pinned TerraDS v2 artifacts; counts of current `.tf`
   files and bytes, not versions or deployment events.
2. **Observed sample**: histories actually recovered from selected upstream Git
   repositories, including inaccessible/unclear-license/failed zero-yield units.
3. **Population estimate**: weighted inference about recoverable history under
   the exact extraction policy, not a fully observed temporal dataset.
4. **Controlled/stress runs**: previous generated/cloned workloads, not production.

## Retrieval and safety

The pinned Zenodo record is 20339474. Published MD5 is verified before accepting
either artifact; SHA-256 is additionally measured. Downloads resume, retry at most
four times, and use bounded range requests for SQLite. The archive is streamed as
an outer tar containing repository-ID tar.gz members; inner archives are streamed
without extracting HCL. Archive paths are checked, links are not followed, and
`.tfvars`/`.tfstate` are excluded from the measurement's `.tf` scope.

Downloaded archives, SQLite, Git repositories, accepted/rejected raw histories,
Spark checkpoints and detailed logs stay under ignored `.cache/`. Compact results
contain only source identities, hashes, counts, sizes, distributions and timings.
The disk guard preserves 20 GiB and rejects a planned allocation above half of free
space. History extraction additionally caps the Git cache at 16 GiB; concurrency
is at most four. Ctrl-C cancels queued jobs; running Git commands have bounded
timeouts. Completed statuses are reused on resume, incomplete statuses retried.
Changing seed, pinned SHA or limits invalidates a cached extraction identity.

## Sampling, without replacement or replacement of failures

The SQLite `Repositories` population supplies clone URLs, GitHub repository IDs,
licenses, end SHAs and module/resource counts. Credential-free GitHub URLs are
canonicalized and deduplicated. The current source byte count is measured from the
archive, not guessed from resources/stars. Three empirical byte-size terciles form
strata (equal values stay together). Sample allocation is approximately proportional
with at least one observation per nonempty stratum. Within each stratum, a
SHA-256 priority of policy version + seed 42 + canonical origin selects a fixed
prefix. The 100-unit pilot is nested in the 500-unit sample.

No unreachable, sparse, secret-rejected or unclear-license repository is replaced.
Every selected origin remains in the denominator. Root licenses are recognized
conservatively at the pinned end SHA using MIT/Apache-2.0/BSD/ISC text evidence;
the detected license, path and SHA-256 are recorded. Unknown evidence is excluded.
TerraDS's CC-BY-4.0 **does not replace source repository licenses**. This gate is
not legal adjudication of per-file/vendor exceptions or historical license changes;
external source is not redistributed in this repository.

## Observation and censoring policy

Fetch the pinned SHA with a bare partial `blob:none` repository, without checkout
or tags. Inspect first-parent history only. The default limit is the latest **2000
first-parent commits** (maximum configurable 10000), not 2000 config revisions.
Record total/selected commits and whether the root is reached. Distinct selected
`.tf` blobs are prefetched in bounded batches; a 50000-blob guard stops unusually
large selected histories visibly. No inference of full-DAG deployment order is made.

An asset is a **file incarnation**. Deletion ends an incarnation; same-path
recreation starts another. Renames are deletion/creation, not continuity. There is
no deletion snapshot. Byte-identical states are suppressed. Phase 11's ordering
policy is retained: selected states are sorted by committer timestamp, with
first-parent ordinal breaking ties. Timestamp/ordinal inversions are measured;
this is not an assertion that clocks reproduce causal deployment order. Assigned
versions are extraction ordinals, not Terraform or repository release versions.

`.tf` is the only accepted format scope. Each selected revision must be nonempty,
UTF-8, nonbinary and at most **1 MiB**. The conservative scanner rejects obvious
private keys, cloud keys, credential/password/token/secret indicators and unsupported
normalization. This also rejects harmless variable references/comments: false
positives are expected. If any selected revision is rejected, its **whole incarnation**
is excluded; no synthetic adjacent pair bridges the missing revision. Deleted and
later safe incarnations remain independent. Unseen/censored revisions are not
certified safe. The scanner is not a complete secret detector.

Provenance records dataset/version, repository ID/URL/license, end SHA, path,
incarnation/asset, module directory, source manifest ID, commit and previous selected
commit, committer offset timestamp and UTC event_time, content SHA-256, assigned
version, seed, stratum, boundary baseline and censoring. The deterministic +5 ms
ingest timestamp is a replay convention, not observed ingestion latency.

## Statistical quantities and uncertainty

For metric y (reachability, license acceptance, accepted repositories, file histories,
revisions or logical bytes), the stratified expansion estimator is:

`T_hat = sum_h N_h * mean(y in sampled stratum h)`.

Every failed/unavailable unit has zero **recoverable** yield. This does not mean it
originally had no history. Bootstrap 2000 repository resamples independently within
strata, seed 42. Deviations from the stratum mean receive finite-population
rescaling `sqrt((1-n_h/N_h)*n_h/(n_h-1))`, matching the estimated without-replacement
mean variance; report 2.5th/97.5th percentiles, with lower
bound clipped to zero. These are approximate design-based intervals, conditional
on the pinned population, extraction caps and conservative license/security policy.
They do not recover unseen heavy tails, deleted histories or selection bias.
Non-census singleton strata cannot support bootstrap uncertainty and fail clearly;
census strata have zero sampling uncertainty.

Full distributions accompany medians/p95s: bytes, versions, uncensored intervals,
textual diff magnitude, repository revision yield and exact hash content returns.
The first pair after a boundary baseline is excluded from interval statistics.
Per-commit bundles key repository + SHA, avoiding cross-fork SHA coalescing.
Hourly/daily windows are reported both globally and separately per repository;
file introductions can contribute large bundles without prior-state diffs.
Such bundles are **correlated IaC changes**, not proven production fleet rollouts.

## Pipeline correctness and performance

Replay only the final actual accepted histories; no cloning/scaling. The existing
producer emits ConfigSnapshots, never precomputed diffs. Native Spark state derives
adjacent pairs. The benchmark harness uses isolated checkpoint/lake identity and
records publisher/progress/state/lag/resource metrics. Search is disabled in the
main replay. Override watermark/timeout to **1000000 hours** and state version cap
to max(32, observed maximum); normal defaults remain unchanged.

Exact validation reads **committed** Parquet and compares snapshot IDs/asset/version/
hash/time and every adjacent pair in both directions using Spark `exceptAll`, plus
duplicate checks. Run existing historical analytics under `analytics/`; totals must
match extraction truth. A separate bounded real multi-version `.tf` file exercises
Kafka → Spark → MinIO → Elasticsearch → FastAPI, including metadata-only responses.

Timing separates historical logical span from physical replay. Micro-batch duration
is not event-time latency. Parquet bytes include raw/processed schemas and overhead;
their ratio to config bytes is not a universal compression ratio. The result is
local replay capacity, not production Terraform or distributed-cluster throughput.

## Tests and reproduction

Fixtures are owned original data: unchanged commits, timestamp ties, rename,
delete/recreate, A-B-A, fake secret rejection and license gates. No external download
occurs in normal unit/Spark CI. Real retrieval/extraction remains explicit.

```sh
make terrads-fetch
make terrads-inspect
make terrads-sample TERRADS_ARGS='--sample-size 100 --seed 42'
make terrads-history TERRADS_ARGS='--sample-size 100 --seed 42 --max-concurrency 4'
make terrads-report
# Expand only after inspecting extraction-100.json and expansion-100.json:
make terrads-sample TERRADS_ARGS='--sample-size 500 --seed 42'
make terrads-history TERRADS_ARGS='--sample-size 500 --seed 42 --max-concurrency 4'
make terrads-report
# Export the existing ignored local Kafka/MinIO environment first:
make terrads-replay TERRADS_ARGS='--benchmark-id phase12-terrads-observed --rate 500'
make terrads-validate TERRADS_ARGS='--benchmark-id phase12-terrads-observed'
make terrads-smoke TERRADS_ARGS='--benchmark-id phase12-terrads-serving'
make phase12-integration-test
```

Do not overwrite a completed benchmark identity or delete user/demo history. A
failed run is retained and a fresh repeat uses a new ID. TerraDS source data is
always treated as data, never infrastructure to deploy.
