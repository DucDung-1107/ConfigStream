# TerraDS v2: verified static configuration corpus

Source: [TerraDS v2, Zenodo record 20339474](https://zenodo.org/records/20339474),
DOI [10.5281/zenodo.20339474](https://doi.org/10.5281/zenodo.20339474).
Published **2026-05-26**, advertised cutoff **January 2026**; retrieved/verified
2026-10-11. Dataset artifact license: **CC-BY-4.0**. Source repository licenses are
separately checked at pinned end commits; artifact licensing is not permission to
redistribute every upstream file.

| Artifact | Exact bytes | Published MD5 | Measured SHA-256 |
| --- | ---: | --- | --- |
| TerraDS.sqlite | 613322752 | faa52a357eefb8729d042f09bd236881 | 728477a499dd949e4dd74ca6230318544839ac465d317ef743da093f3c2e96aa |
| TerraDS.tar.gz | 523682960 | f3cc4d3e1c3b6328111ff6284711dc6b | e3e14764a5a1205f38ed4790d840773ca8c6fccb06adf0e5c5c3c1bf45ae0b79 |

The published tools ZIP is 66165 bytes (MD5
73bb51903d76149c6fbeafc3cf32d703); it is not needed/downloaded/executed here.
The measured SQLite counts match the publication: **82749 repositories, 394004
modules, 2497629 resource declarations**. Neither modules nor resources are revisions.
SQLite provides CloneUrl, LatestCommitSha/At, license and module/resource metadata.
The Data/Managed counts were also checked directly: 396901/2100728. The advertised
January cutoff is **not assumed exact**: 24 accepted sample revisions have committer
timestamps after January, and the sample ends 2026-02-12. Pinned SHAs, not moving
HEAD or an inferred date boundary, define this experiment.

## Complete static archive measurement

The outer archive contains one repository-ID `.tar.gz` member per source repository.
Streaming both levels, without unpacking raw source into Git, measures:

| Quantity | Actually measured |
| --- | ---: |
| Repositories represented by `.tf` files | 82749 |
| `.tf` files | 1513378 |
| Logical Terraform source bytes | 2601501820 |
| Unique current content SHA-256 values | 868124 |
| Unique/current content ratio | 57.3633% |
| Min / median / mean bytes | 0 / 721 / 1719.0033 |
| p90 / p95 / p99 bytes | 3789 / 6043 / 14435.23 |
| Maximum file bytes | 5211802 |

Zero-byte and large files are counted honestly in the full static corpus; accepted
history has stricter size/security bounds. Archive compressed bytes, SQLite bytes,
and logical `.tf` bytes are different quantities. This is **real static scale**, not
1.5 million observed changes, and not an archive of running infrastructure states.
The archive is about 500 MB compressed but the source content is about 2.60 GB.

## Sampling and observed history

The seed-42, source-byte-tercile, without-replacement pilot attempted every one of
100 selected origins. It reached 96, verified root license evidence in 79, accepted
75 repositories, and retained 4 unreachable / 17 license-unknown / 4 no-safe-history
outcomes without substitutes. Pilot evidence: **2583 incarnations / 3687 revisions /
4631412 historical bytes / 2982 distinct contents**. Git cache was 8871794 bytes;
the invocation took 110.513 seconds. No extraction errors or commit-cap censoring
occurred in this pilot. The safety/yield gate permitted a nested 500-unit expansion.

Pilot median/p95 versions is 1/3; only 18.62% of accepted incarnations are multi-version.
The pilot's uncensored median inter-change interval is about 15.38 days, not a
Terraform apply cadence. Full final metrics/estimates and the evidence-based A/B/C
decision are in [Phase 12 acceptance](phase12-acceptance.md).

See [methodology](phase12-methodology.md) for reproducible commands, first-parent
2000-commit cap, file-incarnation/rename policy, UTC timestamps, provenance, license
gate, whole-incarnation security rejection, bootstrap estimator and limitations.
Compact machine-readable evidence is under `workloads/results/terrads/`.
External artifacts/clones/raw histories are ignored; they are never executed.
