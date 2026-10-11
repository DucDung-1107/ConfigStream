# Phase 12 acceptance and empirical decision

Recorded 2026-10-11 on one local machine, one agent. The experiment actually ran;
it is not a proposal. No pipeline redesign, new dependency, Terraform execution,
new rule, deployment, dashboard or ML was introduced. Source and implementation
policies are in [TerraDS](terrads.md) and [methodology](phase12-methodology.md).

## Decision: B — architecture-evaluation only

TerraDS materially improves **static scale, domain variety and empirical realism**.
It does not establish a naturally massive/high-velocity temporal workload. The
observed sample is richer than FRR/NGINX, but 77.48% of accepted file incarnations
have one version. The population inference is about recoverable, permissive,
conservatively filtered first-parent history, including baselines; it is not a
million-revision download, private-enterprise fleet estimate or deployment count.

Keep the architecture/topic only with the explicit framing:
**ConfigStream: An Empirically Grounded Big Data Architecture Evaluation for
Infrastructure Configuration History**. The repository is not renamed. If the
instructor requires an inherently massive *observed temporal* dataset, changing
topic is safer than another phase spent searching for larger headline counts.

## Source verification and complete static corpus

Primary source: [TerraDS v2](https://zenodo.org/records/20339474), DOI
10.5281/zenodo.20339474, published 2026-05-26, advertised January-2026 cutoff,
CC-BY-4.0 artifact license. Attribution: Christoph Bühler, David Spielmann,
Roland Meier and Guido Salvaneschi. SQLite and source archive sizes, published
MD5 and measured SHA-256 are in `source-manifest.json` and [TerraDS](terrads.md).
The tools ZIP was verified in metadata but not downloaded/executed.

Measured SQLite: **82749 repositories / 394004 modules / 2497629 declarations**
(2100728 Managed, 396901 Data). Streaming the complete nested archive measured
**1513378 `.tf` files / 2601501820 source bytes / 868124 distinct contents**.
File min/median/mean/p90/p95/p99/max bytes:
**0 / 721 / 1719.0033 / 3789 / 6043 / 14435.23 / 5211802**.
Static unique content ratio is **57.3633%**.

This is real **static** scale. Files, modules and resources are not revisions or
deployed instances. The static measurement includes empty/large/generated/vendor
files; historical acceptance uses stricter rules. The advertised cutoff is not
assumed exact: 24 accepted revisions have post-January committer timestamps,
and the pinned sample ends 2026-02-12. Those timestamps are retained and reported.

## Sampling and measured funnel

Canonical URL population: 82749, no invalid/duplicate origins found. Seed 42;
policy `terrads-source-bytes-stratified-v1`. Current source-byte tercile boundaries
are 4610 and 14157.3333 bytes. The frame is hashed and tied to both artifact hashes.

| Stratum | Population | Sample | Reachable | Accepted repos | Assets | Revisions | Historical bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| size-0 | 27588 | 167 | 159 | 114 | 422 | 752 | 495736 |
| size-1 | 27578 | 166 | 163 | 132 | 906 | 1555 | 1383086 |
| size-2 | 27583 | 167 | 160 | 129 | 5177 | 7881 | 10399263 |

| Stage | Sampled | Reachable | License verified | Accepted | Unreachable | License unknown | No accepted history |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Pilot | 100 | 96 | 79 | 75 | 4 | 17 | 4 |
| Final nested expansion | 500 | 482 | 397 | 375 | 18 | 85 | 22 |

No failed/sparse unit was replaced. Pilot: 2583 assets, 3687 revisions, 4631412
bytes, 110.513 seconds, 8871794 Git-cache bytes. The measured safety/yield gate
permitted expansion. The 500 stage reused the pilot and took **346.281 seconds**;
final Git cache **35307220 bytes**. A 1000-unit stage was optional and not run:
the bounded 500-unit experiment already answers the question without assuming
that additional clones would turn sparse histories into production telemetry.

Verified root license evidence: MIT 234, Apache-2.0 158, BSD-3-Clause 3,
BSD-2-Clause 2. Unknown root evidence excludes 85, despite the dataset's permissive
repository selection. License checks are not per-file/vendor legal adjudication
or a guarantee that all old revisions had the same terms. No upstream raw source
is committed or redistributed here.

261 repositories contain security-rejected incarnations: 18 have no accepted
history, 243 also retain separate safe incarnations. Whole-incarnation exclusions:
3348 conservative credential-pattern, 289 empty, 4 non-UTF-8 and 2 binary.
These are not 3348 proven credential leaks; harmless secret variable references
can trigger the scanner. Four other no-history repositories are not wholly
credential-rejected. Extraction errors, missing-pinned-SHA statuses, timeouts and
disk-guard stops: **0**. Unreachable outcomes: **18**, retained in the denominator.

The selected first-parent graphs contain **10397 commits**, maximum 339/repository.
All reachable graph inspections reached the root; the 2000-commit cap did not
censor this sample. Full DAG branches, rename continuity and deletion snapshots
are intentionally not observed. Accepted content max is 47462 bytes; the 1 MiB
bound excludes larger files if encountered. First-pair boundary intervals are
excluded. Timestamp/first-parent order inversions in accepted history: **0**.

## Observed history, not generated history

Final actually observed accepted sample:
**375 repositories / 6505 incarnations / 10188 revisions / 8409 unique contents /
12278085 bytes**, unique ratio **82.5383%**. UTC span:
2015-04-14T22:45:51Z to 2026-02-12T04:15:32Z, **3956.229 days**.

| Statistic | FRR/NGINX | Accepted Terraform sample |
| --- | ---: | ---: |
| Median / p95 config bytes | 298 / 1267 | 645 / 4115.9 |
| Median / p95 versions | 1 / 2.75 | 1 / 4 |
| Mean / maximum versions | 1.325 / 7 | 1.566 / 75 |
| Median uncensored inter-change | 125.986 days | 6.713 days |
| Median / p95 changed lines | 0 / 4 | 1 / 7 |
| Largest changed-file commit/hour bundle | 136 | 484 |
| Unique contents/revisions | 87.9208% | 82.5383% |

Terraform version bins: **5040 one-version; 1285 two-to-five; 172 six-to-twenty;
8 twenty-one-to-one-hundred; 0 above one hundred**. Multi-version fraction 22.52%.
Multi-version files contain 5148 revisions (mean 3.514/file).
Revisions/sample repo 20.376; reachable repo 21.137; accepted repo 27.168.
Historical bytes/sample repo 24556.17. Repository-yield median/p95/max: 5/77.25/893.

Uncensored inter-change sample n=3502. Seconds p10/p25/p50/p75/p90/p95/max:
982.8 / 23713 / 579975.5 / 3921771 / 18143052 / 32832070.7 / 182013779.
Calendar change-count medians are zero for day/week/month; p95 is 0/0/1.
These calendar counts assume file observation until deletion or pinned repository
end; they do not measure devices or Terraform applies.

Adjacent textual comparisons: 3683. Added mean/median/p95/max:
6.9805/2/27/539; removed 3.9709/1/16/192; changed 2.0796/1/7/166.
Raw changed-line mean/p95 is 2.6392/9. Normalization yields equal content for
159 adjacent pairs. This is deterministic text normalization, not semantic HCL
impact or a Terraform plan.

Exact earlier-content returns: **59 in 53 files**, median time since previous
change 70317 seconds. They are content reversions, not demonstrated operational
rollbacks. Repository+commit bundles: 2533, median/p95/max files 2/11/484.
Repository hourly/daily median/p95/max: 2/13/484 and 2/16/484.
The maximum 484 includes **new-file introductions**. For existing-file revisions
only, maximum commit/hour/day correlation is **39**; p95 is 5/6/8.
Do not turn a bulk import or independent repository time coincidence into a
proven production rollout. The full distributions and safe commit examples are
in `history-summary.json`.

## Population inference — not observations

Estimator `sum_h N_h * mean_h(y)`, repository bootstrap 2000 repetitions, seed 42,
finite-population/empirical-variance rescaling described in methodology. Weights
are N_h/n_h (about 165–166). Failed/unavailable units have zero **recoverable**
yield; no inference recovers their underlying history. Intervals account for
sampling, not unknown licenses, scanner false positives or unseen extreme repos.

| Recoverable quantity | Point estimate | Approximate bootstrap 95% interval |
| --- | ---: | ---: |
| Reachable repos | 79773 | 78286–81095 |
| License-verified repos | 65709 | 62739–68686 |
| Accepted repos | 62069 | 59090–65211 |
| File-incarnation histories | 1075302 | 819121–1398095 |
| Historical states including baselines | 1684251 | 1308542–2135728 |
| Adjacent comparisons | 608949 | 437572–818605 |
| Historical logical bytes | 2029291960 | 1527853314–2597583718 |

Correct claim: **the stratified sample estimates** these recoverable quantities
in the TerraDS population under the pinned policy. Wrong claim: we downloaded
1.68 million historical states. Only **10188** were actually observed/replayed.
Estimated filtered historical bytes can be less than unrestricted static bytes:
licensing and whole-history filtering change the estimand. Descriptive file-level
percentiles are sample descriptions, not weighted population quantile estimates.

## Real replay and exact ground truth

Run `phase12-terrads-observed`, local[2], six Kafka partitions, target 500/s,
2 GiB Spark driver, max 10000 offsets/batch, isolated checkpoint and
`benchmarks/phase12-terrads-observed` lake prefix. Watermark and timeout 1000000
hours; version cap **75** (observed maximum). Normal defaults are unchanged.

| Metric | Measured |
| --- | ---: |
| Accepted/normalized/raw snapshots | 10188 each |
| Adjacent diffs | 3683 |
| Alerts / diagnostics | 0 / 0 |
| Logical configuration bytes | 12278085 |
| Kafka serialized value bytes | 17264648 |
| Committed raw+processed Parquet bytes | 29402997 |
| Publisher seconds / achieved rate | 20.375 / 500.023 snapshots/s |
| Streaming including drain / logical accepted rate | 68.742 s / 148.205 snapshots/s |
| Startup / drain / total harness seconds | 6.422 / 48.367 / 76.071 |
| Active batches | 3 |
| Native processedRowsPerSecond mean | 364.693 |
| Native inputRowsPerSecond mean | 606.832 |
| Micro-batch p50 / p95 | 23681 / 32044 ms |
| Peak / final measured Kafka lag | 10061 / 0 |
| Maximum operator state rows / exposed memory | 13010 / 2719444 bytes |
| Watermark drops / dedup drops | 0 / 0 |
| Sampled Java peak RSS / CPU | 1766965248 bytes / 396% |

Native numInputRows sum **30564** reflects three branches reading Kafka in the
multi-sink query, not 30564 logical snapshots. Watermark remains 1970-01-01Z with
this intentionally very long replay retention. Exposed state memory is not JVM
RSS. Thirteen resource samples include Kafka/MinIO container CPU/RAM text; Git
transport traffic is **not instrumented**, and 35 MB cache is not network bytes.

Exact committed-Parquet comparison: zero missing/unexpected snapshots, zero
missing/unexpected pairs, zero duplicate snapshot IDs or pairs. Thus every
expected same-asset vN→vN+1 pair appears once logically, with exact hash/time.
No precomputed diffs were published. A separate service-free fixture also replays
versions backwards plus a duplicate, producing the same three adjacent pairs.

The high input rate is **accelerated replay of accumulated history**, not expected
per-device configuration frequency or Terraform apply frequency. Parquet/config
ratio ~2.395 includes raw/processed datasets, dates, IDs, schema and small-file
overhead; it is not a universal compression ratio or production storage claim.

## Historical analytics and serving compatibility

Existing Spark batch jobs read only committed history and persist typed Parquet
under the run's `analytics/` zone. Frequency totals match 10188 snapshots / 3683
diffs / 0 alerts. Outputs:

| Job | Rows | Recorded job seconds | Additional result |
| --- | ---: | ---: | --- |
| change-frequency | 9229 | 12.827 | Exact totals |
| fleet-activity | 1185 | 13.214 | Maximum 40 existing-diff assets/day |
| diff-magnitude | 1 | 5.403 | 3683 comparisons; p95 changed 7 |
| asset-summary | 6505 | 12.613 | One row per accepted incarnation |
| anomalies | 278158 | 13.935 | 5 statistical outlier windows |
| fleet-bursts | 3958 | 12.749 | 102 statistical outlier windows |
| alert-trends | 0 | 0.295 | Unchanged rules match none |
| diagnostic-trends | 0 | 0.468 | No streaming diagnostics |

Anomaly rows include generated zero-count windows, not additional observed data.
Outliers are not incidents. Recorded job seconds are verification timings, not
a matched historical-performance benchmark (regression also ran during part of
validation). Fleet activity counts existing diffs; empirical introduction-inclusive
bundles use a different denominator.

Serving smoke uses **one actual six-version Terraform file**, no mutations:
Kafka → Spark → MinIO → Elasticsearch → FastAPI. Both TestClient and an actual
loopback Uvicorn/TCP server return healthz/readyz 200, latest version 6, six
snapshot metadata documents, five adjacent diffs (1→2 through 5→6), zero alerts
and diagnostics. Raw/normalized bodies and diff details are absent. Its isolated
lake stores 90090 Parquet bytes. The temporary Uvicorn process is stopped.

## Cross-phase comparisons

| Evidence | Repositories/assets | States/files | Logical bytes | Observed? |
| --- | ---: | ---: | ---: | --- |
| FRR/NGINX | 2 repos / 1906 incarnations | 2525 | 1133998 | Yes |
| TerraDS static | 82749 repos | 1513378 current files | 2601501820 | Real static, no temporal mining |
| TerraDS Git sample | 375 accepted repos / 6505 incarnations | 10188 | 12278085 | Yes |
| TerraDS history estimate | ~62069 accepted repos / ~1075302 incarnations | ~1684251 estimated | ~2029291960 estimated | No, inferred |
| Phase 11 calibrated | 76240 cloned assets | 100518 | 45002724 | No |
| Phase 10 one-million stress | 10000 generated assets | 1000000 | 1144675896 | No |

The controlled 100×100 reference has median 100 versions, 9900 comparisons from
10000 states, 12542677 logical bytes, 7700 unique contents and p95 changed lines
zero. Terraform has median 1, 3683/10188 comparisons, p95 changed lines 7, and a
much longer original observation span. The 1M run has 990000 comparisons,
250000 synthetic rule matches and 680000 state-eviction diagnostics under its
different version cap. These are **not matched throughput experiments**.

Recomputed content reuse from committed summaries: FRR/NGINX 2220/2525=87.9208%,
Phase 11 calibrated 2220/100518=2.2086%, 1M stress 26800/1000000=2.68%,
Terraform 8409/10188=82.5383%. Whole-lake stored/config ratios approximately
4.73 / 0.97 / 0.77 / 2.395 respectively include different schemas, diagnostics,
partition overhead and heavy synthetic reuse. None is production compression.

## Resource safety and reproducibility artifacts

Host: Apple M3 Pro, 12 cores, 18 GiB RAM, macOS arm64; approximately 926 GiB disk
and 631 GiB free at the resumed source verification. Initial earlier check was
about 633 GiB. Final extraction free disk 674885586944 bytes (~628.54 GiB), well
above reserve. Docker/Lima has 2 vCPUs, 6 GiB RAM and 24 GiB configured disk;
6.9 GiB was available before the small replay. Java 21/Python 3.12.14/Spark
4.0.2/Kafka 4.1.2 are in replay metadata. No distributed-cluster claim is made.

Accepted artifact bytes total 1137005712. Last resumed invocation measured
429216912 transfer bytes, SQLite 303.482 s and archive 65.310 s concurrently.
Earlier interrupted attempts/prefix traffic/time were not aggregated; there is
no invented whole-download throughput. Host cache remains ignored and resumable.

Committed compact artifacts in `workloads/results/terrads/`:
source-manifest; static-summary JSON/CSV; sample-100/sample-500/final manifests;
extraction-100/extraction-500/final summaries; expansion gates; history-100/final
summaries; repository-summary JSON/CSV; population-estimate; replay-summary;
replay-validation with output schemas; serving-smoke; comparison CSV and measured
CDF SVGs. Plots cover static/observed bytes, versions, intervals, textual magnitude,
correlated files and revisions per repository. Raw configs/Git/SQLite are absent.

Implementation: `workloads/terrads.py`, `terrads_sampling.py`, `terrads_history.py`,
`terrads_main.py`, `terrads_validation.py`; reuse parameterized Phase 11 extraction,
scanner/statistics/plotter and existing benchmark/analytics. Add owned Git fixture,
unit tests, native Spark fixture test, service integration test and Make targets.
No dependency or `uv.lock` change is needed; locked dependencies were verified.

## Local regression and exact acceptance commands

| Command | Passing cases |
| --- | ---: |
| make check | 782 |
| make api-test | 80 |
| make spark-test | 60 |
| make integration-test | 3 |
| make spark-integration-test | 3 |
| make lake-integration-test | 1 |
| make search-integration-test | 2 |
| make api-integration-test | 2 |
| make phase10-integration-test | 1 |
| make phase11-integration-test | 1 |
| make phase12-integration-test | 1 |

Total **936 distinct project cases**, 40 more than Phase 11: 38 pure unit,
one native Spark and one service integration case. Focused reruns are not counted
again. Pure CI downloads no external corpus and uses no service; existing workflow
already discovers the new unit/Spark cases. Commit SHA and actual completed CI run
URL/conclusion are reported in the handoff, not asserted before a workflow finishes.

Failures are retained locally: an initial new Spark assertion expected lake output
without enabling projection; the focused lake-session attempt would download
unneeded connectors and was stopped; an initial fixture integration compared
Python host-local datetime conversion with UTC. Final tests enable lake projection
without S3 dependencies and compare SQL UTC strings. A mistyped focused test path
ran no tests and was corrected. None of these failures was a successful measurement
or an excuse to weaken existing tests. No real history extraction/replay failed.

Executed workflow (in addition to targeted reads/lint/fixture reruns):

```sh
limactl start k --tty=false
source .cache/local-kafka-env.sh
set -a; source .cache/minio.env; set +a
make search-up kafka-topics lake-init
.venv/bin/python -u -m workloads.terrads_main fetch
make terrads-inspect
make terrads-sample TERRADS_ARGS='--sample-size 100 --seed 42'
make terrads-history TERRADS_ARGS='--sample-size 100 --seed 42 --max-concurrency 4'
make terrads-report
make terrads-sample TERRADS_ARGS='--sample-size 500 --seed 42'
make terrads-history TERRADS_ARGS='--sample-size 500 --seed 42 --max-concurrency 4'
make terrads-report
make terrads-replay TERRADS_ARGS='--benchmark-id phase12-terrads-observed --rate 500'
make terrads-validate TERRADS_ARGS='--benchmark-id phase12-terrads-observed'
make terrads-smoke TERRADS_ARGS='--benchmark-id phase12-terrads-serving'
CONFIGSTREAM_SEARCH_INDEX_PREFIX=phase12-terrads-serving \
  CONFIGSTREAM_API_PORT=8001 .venv/bin/python -m api.main
# HTTP GETs: healthz, readyz, snapshots, diffs, alerts and the measured asset's latest snapshot.
.tools/uv/bin/uv lock --check
make check
make api-test
make spark-test
make integration-test
make spark-integration-test
make lake-integration-test
make search-integration-test
make api-integration-test
make phase10-integration-test
make phase11-integration-test
make phase12-integration-test
```

## Safe claims, unsafe claims and instructor conclusion

Safe: a substantially larger real static IaC population was measured; 500 fixed
origins were attempted; accepted observed Terraform history genuinely traversed
the existing stateful pipeline; exact pairs matched; weighted *recoverable-history*
inference and uncertainty are disclosed; local controlled replay capacity works.

Unsafe: 1M production snapshots; 1.68M actually mined TerraDS revisions; every
resource is a deployment; each file is a real device; Git interval is apply cadence;
484-file import is fleet rollout; a text diff is infrastructure impact; zero rules
means no risk; scanner finds all secrets; local results show horizontal scalability.

Git is a public proxy for configuration evolution. Templates, examples, forks,
vendored/generated code, public-repository bias, root-license uncertainty,
secret-filter false positives, deleted repos, omitted branches, incarnation resets
and clock semantics constrain external validity. Sparse retained history must not
be generalized into a claim that all private Terraform workloads are sparse.

**Instructor-ready Vietnamese:** Ban đầu lịch sử FRR/NGINX công khai khá nhỏ và
thưa. Nhóm đã đo toàn bộ TerraDS: 1,51 triệu file Terraform, khoảng 2,60 GB nội dung,
và lấy mẫu cố định 500 repository để khai thác Git. Sau kiểm tra license/an toàn,
nhóm thực sự quan sát 10188 phiên bản của 6505 file-incarnation trong 375 repo;
77,5% chỉ có một phiên bản. Ước lượng có trọng số là khoảng 1,68 triệu trạng thái
trong phần lịch sử có thể thu hồi, không phải 1,68 triệu bản ghi đã tải. Kafka–Spark–
MinIO và analytics xử lý đúng toàn bộ mẫu thực, nhưng kết quả chưa chứng minh
configuration history tự nhiên có volume/velocity rất lớn. Nhóm đề nghị giữ đề tài
theo hướng đánh giá kiến trúc Big Data với workload có căn cứ thực nghiệm; nếu môn
học bắt buộc dữ liệu thời gian tự nhiên cực lớn thì nên đổi đề tài, không tiếp tục
nhân dữ liệu hoặc săn corpus chỉ để có con số đẹp.

Recommendation: stop further data-search phases, obtain instructor agreement on
this framing, then focus on report/defense and reproducible evidence. No Phase 13
or deployment is implemented.
