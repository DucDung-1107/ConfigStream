# Empirical validation and calibrated workloads

## Phase 12 external check

The new [observed Terraform sample](phase12-acceptance.md) has median/p95 versions
1/4, median/p95 config bytes 645/4115.9, median uncensored interval 6.71 days and
p95 textual changed lines 7. The existing controlled 100×100 reference instead
has median 100 versions and p95 changed lines zero. The reference is useful
capacity ground truth, not a matched natural frequency/magnitude distribution.

The accepted Terraform sample's content uniqueness is 82.54%, versus 2.21% for
Phase 11 cloned histories and 2.68% in the Phase 10 million-record workload.
Storage/throughput comparisons must disclose reuse and differing output sets.
`workloads/results/terrads/comparison.csv` and CDFs reproduce the comparison.
No Terraform-calibrated synthetic run or artificial Terraform alerts were added;
Phase 12 replays all 10188 accepted **actual** states without scaling.

Observed data, calibrated clones and capacity stress are **separate evidence**.
Git history is an observable public proxy for configuration evolution, not router
deployment frequency. See [source decisions](real-history-sources.md) and
[external validity](external-validity.md).

## Measured distributions

`empirical-v1` uses linear interpolation at `(n-1)*p`. Histograms retain actual
value/count observations; no manually entered plot values. Tiny-sample percentiles
are descriptive only, especially NGINX's one uncensored inter-change interval.
CDF plots use log1p horizontal axes and include the selected source populations.

| Statistic | FRRouting | NGINX | Combined |
| --- | ---: | ---: | ---: |
| Observed file-incarnation assets | 1900 | 6 | 1906 |
| Observed revisions | 2516 | 9 | 2525 |
| Revision-content bytes | 1,109,458 | 24,540 | 1,133,998 |
| Unique contents | 2211 | 9 | 2220 |
| Median / p95 config bytes | 297 / 1262 | 2630 / 5325.4 | 298 / 1267 |
| Maximum config bytes | 12241 | 5349 | 12241 |
| Median / p95 / max versions per asset | 1 / 2.05 / 7 | 1 / 2.75 / 3 | 1 / 2.75 / 7 |
| Uncensored interval samples | 307 | 1 | 308 |
| Median inter-change hours | 2998.3783 | 4080.3033 | 3023.6674 |
| p95 changed replacement lines | 4 | 0 | 4 |
| Median / p95 added lines | 1 / 12 | 1 / 1 | 1 / 12 |
| Median / p95 removed lines | 1 / 6 | 0 / 1.8 | 1 / 6 |
| Max correlated selected files/hour | 136 | 1 | 136 |
| Exact content reversions / affected assets | 4 / 4 | 0 / 0 | 4 / 4 |

Full min/mean/p10/p25/p50/p75/p90/p95/p99/max and histograms are in
`workloads/results/real-history/observed-summary.json`; source CSV and five CDF SVGs
are generated alongside it. First-known baseline size distributions are separate
from all revision sizes. The maximum exceeds Batfish's 8015-byte maximum, but the
network corpus median is smaller; this is not a new production-sized corpus claim.

Combined version bins: 1422 assets have one version, 482 have 2–5, two have 6–20,
and none has more than 20. Most public files have sparse observed changes.
Combined snapshot span is 2219.8279 logical days; repository observation boundaries
and each deleted incarnation's end can differ from last-change time.

### Frequency and censoring

Changes/day/week/month count **transitions after the first accepted version**.
Calendar bins intersect the observed file-incarnation interval; first/last partial
bins are included and clearly are not full deployment observation periods.
Weekly bins begin Monday UTC; months are calendar months. Idle Git bins are zero.
The medians are zero for all three period lengths in both sources. Histograms,
sample-bin counts and nonzero tails, not just a mean, are retained in the JSON.
Inter-change intervals exclude a pair whose previous version is a left-censored
boundary baseline. File creation is observable when it occurs inside the selected
range, but a creation does not yield a fake baseline ConfigDiff.

### Diff, reversion and correlated edits

The exact Phase 5 normalizer/difflib engine computes 619 adjacent comparisons.
Raw and normalized replacement counts are both measured. Zero raw-changing pairs
became normalized-equal in this accepted selection. Supported secret-value rotations
are not measurable here because suspicious paths are rejected before analysis.
Normalization is not a semantic network-impact parser.

`changed` counts replacement pairs only; a delete/insert or moved setting can have
`changed=0` while `added`/`removed` are nonzero. Never interpret that as no change.
Reversion means a nonconsecutive exact raw hash return (A-B-A); elapsed time is
from the immediately previous change, not a measured incident-recovery duration.
Four returns are observed, in four FRRouting incarnations; median return interval
is 34,493,810 seconds. No operational rollback conclusion is drawn.

Bundles and 5-minute/hour/day windows count selected files with non-baseline revisions,
including creation. They do not classify deployments. The largest accepted bundle
is commit `081bda813eb8b122a153df90adfad99578edabb9`, 2026-05-11T19:52:36Z,
136 files. Safe top-five commit metadata and distributions of bundle revision bytes
and added+removed magnitude are committed. Bundle bytes mean bytes of resulting
revisions, not a Git pack size or bytes physically edited. Deleted files themselves
are not snapshot records. Spark fleet activity instead counts assets with derived diffs;
its counts may differ from bundles that include creation.

## Calibrated profile

`observed-replay`: all and only accepted observed histories; multiplier must be 1.

`observed-calibrated`: seed a whole-history bootstrap **with replacement** from the
1906 accepted file-incarnation histories, then clone `1906 × multiplier` assets.
For multiplier 40/seed 42 the actual output is **76,240 assets / 100,518 snapshots**,
not an assumed fixed number of versions/asset. Original content bytes, assigned
version counts and original timestamp spacings are retained. New IDs are deterministic
UUID5; Git originals/provenance remain linked through source snapshot IDs.

This samples the measured joint size/version/interval/diff histories without invented
probabilities or unsupported vendor mutations. It preserves the original temporal
alignment while scaling bundles by resampling multiplicity. It does not infer an
enterprise population, learn deployment causality or reproduce every file relationship.
Weighted histogram calculation is exactly equivalent to materializing small clone
fixtures, tested automatically; large calendar-bin counts need not be expanded per clone.

The manifest records source specification IDs and measured aggregate checksums,
`empirical-v1`, seed, multiplier, sampling algorithm and preserved characteristics.
Physical target replay rate is recorded separately from logical time in generation/
benchmark metadata. Historical timestamp replay uses deliberately long watermark and
timeout settings; normal streaming defaults are unchanged. Content is never padded.

| Metric | Observed combined | Phase 10 controlled reference | Calibrated multiplier 40 |
| --- | ---: | ---: | ---: |
| Assets / snapshots | 1906 / 2525 | 100 / 10000 | 76240 / 100518 |
| Median config bytes | 298 | 632 | 296 |
| Median versions/asset | 1 | 100 | 1 |
| Median / p95 replacement lines | 0 / 4 | 0 / 0 | 0 / 3 |
| Median inter-change hours | 3023.6674 | 24 | 2998.3783 |
| p95 correlated files/hour | 15.1 | 5 | 610.2 |
| Unique snapshot content ratio | 87.9208% | 77% | 2.2086% |

The controlled reference is a **measured 100-assets × 100-versions seeded sample**
of the existing Phase 10 stress profile, not a measurement of the complete million
run's medians. Its 12,542,677 content bytes are independent of the million run's
1,144,675,896 bytes. Different baseline sampling can change size statistics.
The million run's asset/version/input counts remain in the original benchmark evidence.

The daily controlled cadence strongly overstates this selection's observed change
frequency. Selected public test files are mostly smaller than Batfish, although NGINX
templates are larger on median and FRRouting includes a 12,241-byte revision. Cloning
amplifies correlated edits and reduces entropy; it is not 76,240 observed devices.

## Real replay experiments

Both replay variants use unchanged native Kafka→Spark state→committed MinIO logic.
They publish snapshots, not precomputed diffs. Standard `rules/default.yaml` is unchanged;
observed and calibrated replays produce **zero alerts**, a valid negative result.
ES/API live indexing is OFF; services may be running idle. The local driver uses 2g,
local[2], six input partitions, 10k max offsets/batch and max history cap 32.
The historical replay watermark and state timeout are 1,000,000 hours to avoid
discarding retained historical timestamps during this correctness experiment.
These are **replay-specific**, not production retention recommendations.

| Run | Snapshots | Diffs | Logical bytes | Committed Parquet bytes | Streaming wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| phase11-observed | 2525 | 619 | 1,133,998 | 5,361,869 | 33.415 |
| phase11-calibrated-40 | 100518 | 24278 | 45,002,724 | 43,656,587 | 232.340 |
| phase11-calibrated-40-isolated | 100518 | 24278 | 45,002,724 | 43,651,708 | 226.090 |
| Phase 10 main, existing evidence | 1000000 | 990000 | 1,144,675,896 | 876,030,173 | 1065.618 |

The first calibrated run overlapped a service-free Spark test process for part of
its duration, so it is retained but not an isolated performance comparison. The
second ran without a competing Spark test/query. Pure report preparation near startup
is not an exclusive-machine steady-state benchmark. There is one such repeat, not
a statistically powered causal experiment. Startup is separately recorded; original
logical span is 2219.828 days, not 33 or 226 seconds.

Observed producer rate is 500.015 snapshots/s; calibrated isolated producer is
999.995. Logical accepted/streaming-wall rates are about 75.6 and 444.6 respectively.
Native Spark row-rate counters can count unioned branches three times and are not
logical snapshot rates. Isolated micro-batch p50/p95 are 20.879/21.980 seconds;
max exposed state operator rows 152480, memory 18,272,210 bytes. No native watermark
drops or duplicate drops were observed. Native metrics are not total pipeline memory
or simulated-event-time end-to-end latency.

Calibration materially changes output mix: mostly baselines and fewer adjacent
comparisons, many more asset keys, smaller source bodies and strong content reuse.
The throughput/storage differences therefore cannot be attributed to calibration
alone at equal volume. Sparse event dates/file overhead, state key count, rules and
replay retention differ from Phase 10. No horizontal/production capacity claim follows.

Committed Parquet totals include raw and processed datasets; observed stored/logical
ratio is about 4.73 (small-file/schema overhead), scaled ratio about 0.97 and Phase 10
about 0.77. These are **not raw-only compression ratios**. Repeated baseline/history
content affects compression, so none should be presented as production storage behavior.

Native Spark re-reading of committed history measures 2220 unique raw hashes in
2525 observed snapshots (87.9208%), 2220 in 100518 calibrated snapshots (2.2086%),
and **26800 in the complete Phase 10 million run (2.68%)**. These are actual
distinct-content measurements, not just baseline file counts. A high reuse rate
is another reason not to extrapolate storage results to private production history.

## Historical Spark validation

`workloads/results/real-history/replay-analytics.json` records native Spark batch
validation over committed MinIO inputs. Joins against old/current snapshot IDs found
**zero non-adjacent or cross-asset diffs** in observed, calibrated and million-run
histories. Observed frequency totals are exactly 2525 snapshots / 619 diffs / zero
alerts; calibrated totals are 100518 / 24278 / zero; Phase 10 totals are
1000000 / 990000 / 250000. No ground-truth diffs are fed into streaming.

| Job | Observed output rows / seconds | Calibrated output rows / seconds | Phase 10 main output rows / seconds |
| --- | ---: | ---: | ---: |
| Change frequency | 2520 / 4.105 | 100346 / 4.630 | 1000000 / 13.342 |
| Fleet activity | 283 / 3.119 | 283 / 4.383 | 100 / 13.101 |
| Diff magnitude | 1 / 1.218 | 1 / 1.458 | 1 / 4.356 |
| Asset summary | 1906 / 2.974 | 76240 / 4.282 | 10000 / 10.739 |

Observed jobs also persisted alert/diagnostic trends (both empty), asset-frequency
anomalies (296126 rows, **zero outlier windows**) and fleet bursts (2221 rows,
**25 statistical outlier windows**). These are retrospective statistical descriptions,
not incidents or verified deployments. Maximum derived-diff assets/day is 136 for
observed history and 5410 for cloned calibrated history.

All outputs are typed Parquet under the existing deterministic analytics run contract.
Timings are warm validation executions and include each job's existing output work;
they are not cold-start, repeated equal-size scaling benchmarks. No Elasticsearch
scan or Python collection of full history is used.

## Commands and artifacts

```sh
python -m workloads.observed_main benchmark --benchmark-id phase11-observed --rate 500
python -m workloads.observed_main benchmark --benchmark-id phase11-calibrated-40 \
  --profile observed-calibrated --multiplier 40 --rate 1000
python -m workloads.observed_main benchmark --benchmark-id phase11-calibrated-40-isolated \
  --profile observed-calibrated --multiplier 40 --rate 1000
python -m workloads.observed_analysis \
  --runs phase11-observed phase11-calibrated-40-isolated main-1m-2g-50k-20261011
```

Use new benchmark IDs for fresh runs; old checkpoints/evidence are never deleted.
`workloads/results/real-history/calibration/` contains generated comparison JSON/CSV,
five CDF overlays and calibration manifest. Compact replay results and committed
analytics validation are recorded separately. The observed analytics includes frequency,
diff magnitude, fleet activity and asset summaries; outlier windows are statistical
descriptions, not incidents. No rule was added just to produce impressive alert counts.
