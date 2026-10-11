# Phase 11 acceptance and evidence

Recorded on 2026-10-11, single local machine, one coding agent. Phase 0–10 architecture
is preserved. No new dependency, schema, rule, dashboard or deployment is added.
Repository: [kongwoang/ConfigStream](https://github.com/kongwoang/ConfigStream).

## Sources, observations and limits

[Source decisions](real-history-sources.md) record accepted FRRouting and NGINX,
the considered Kubernetes/Batfish alternatives, exact start/end SHAs, license
evidence, retrieval date and measured scope counts. FRRouting topotest configurations
are ISC-style; NGINX's pinned LICENSE is BSD-2-Clause. External clones/raw histories
remain ignored. No arbitrary GitHub secret harvesting occurred.

Accepted history: **1882 distinct paths / 1906 file-incarnation assets / 2525
revisions / 2220 distinct contents / 1,133,998 content bytes**. Combined snapshot
span is 2219.8279 days. Median/p95 size is 298/1267 bytes, maximum 12241 bytes;
median/p95 versions is 1/2.75, maximum 7. There are 308 uncensored inter-change
samples, median 3023.6674 hours (about 126 days). NGINX contributes only one such
interval, so its frequency estimate is especially weak.

Calendar day/week/month change-count medians are zero. Adjacent comparison count
is 619; median/p95 replacement lines is 0/4, median added/removed lines both 1.
Replacement count zero is not equivalent to empty diff. Four exact content returns
occur in four FRRouting assets; they are **content reversions**, not proven
operational rollbacks. Largest selected correlated commit/hour bundle is 136 files.
Full period, interval, size, diff, revert and bundle distributions are in the compact
JSON; no figures were estimated manually.

Provenance binds repository/path/incarnation, commit SHA, previous selected commit,
original offset timestamp, committer-time UTC, content SHA-256, assigned version,
source manifest/license, boundary flag and observation end. Committer time is a
Git observation proxy, not a deployment timestamp. ConfigStream assigns versions;
deterministic ingest time (+5 ms) is a replay convention.

The security filter rejects whole path histories if any selected revision is unsafe.
FRRouting has 239 rejected paths (200 conservative credential-pattern, 39
empty/oversized); NGINX has zero. Public rejection audit stores only source/path
hash/category, not rejected body/path. False positives and undetected secrets remain
possible. Search/API security policies are unchanged and configuration bodies are
never added to Elasticsearch or HTTP responses.

## Replay and calibration

Existing producer publishes snapshots; native Spark state derives adjacent diffs.
Standard rules are unchanged. Both observed and scaled public histories yield
**zero alerts and zero diagnostics**, not artificially injected rule matches.

| Run | Accepted | Adjacent diffs | Logical bytes | All committed Parquet bytes | Streaming wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| observed | 2525 | 619 | 1133998 | 5361869 | 33.415 |
| calibrated ×40, initial | 100518 | 24278 | 45002724 | 43656587 | 232.340 |
| calibrated ×40, isolated Spark repeat | 100518 | 24278 | 45002724 | 43651708 | 226.090 |

The calibrated profile uses seeded whole-history bootstrap with replacement, not
arbitrary mutation percentages. It clones **76240 assets**, preserves actual content,
version counts and timestamp spacing, and records source specification/checksum,
statistical policy, seed, multiplier and physical replay rate. Original logical span
remains about 6.08 years while physical replay takes minutes. These are not observed
76240 devices. Seed/revision equality reproduces IDs and histories.

Initial scaled replay overlapped a Spark regression process for part of its duration;
that result is retained and labelled, not cherry-picked away. The repeat had no
competing Spark query/test, but report preparation near startup means it is not an
exclusive-machine steady-state experiment. Only one such repeat was performed.

Observed/scaled target rates are 500/1000 snapshots/s; achieved producer rates
are 500.015/999.995. Scaled accepted/streaming-wall rate is about 444.6/s including
drain; native Spark mean processedRowsPerSecond is 1365.416, counting union branches,
not logical snapshots. Scaled trigger p50/p95 is 20879/21980 ms. Exposed maximum state
operator rows/memory are 152480/18272210 bytes, not total process memory. No watermark
or dedup drops are reported with this ordered input and replay-specific long retention.

Peak sampled lag is 2525 observed / 62910 isolated scaled, final lag zero. Scaled
post-publisher drain is 125.571 s, evidence of backlog at the selected physical rate,
not a claim that 1000/s was sustained. Forty-four resource samples recorded peak
aggregate Java RSS 2199470080 bytes, Java `ps` CPU 347.1% (multi-core accounting),
Kafka/MinIO container memory 862663475/483917824 bytes. Sampling is approximate.
No additional saturation/OOM run was attempted; existing Phase 10 failure evidence
is not erased.

Machine metadata in each replay JSON: macOS 27 arm64, Apple M3 Pro, 12 cores,
19327352832 bytes RAM, Python 3.12.14, Spark 4.0.2, Java 21.0.12.1, Kafka 4.1.2,
Docker 29.8.1. Docker engine has two CPUs/about 6.19 GB RAM. Storage hardware was
not probed. local[2], 2g driver, six Kafka partitions, 10k offsets/batch, cap 32;
watermark/timeout 1000000 hours are historical-replay overrides, not normal defaults.

## Historical analytics and comparison

Existing `read_committed` history, not Elasticsearch, supplies native Spark batch
analytics. All observed/calibrated/million-run diff joins have zero non-adjacent or
cross-asset pairs. Frequency totals match accepted/diff/alert counts exactly.
Observed persisted outputs include frequency, fleet activity, diff magnitude,
empty alert/diagnostic trends, asset summaries and statistical outlier tables.
Zero asset-frequency outlier windows and 25 fleet outlier windows are retrospective
statistics, **not incidents**. See [calibration](workload-calibration.md) for output
schemas/counts, warm validation timings and full Phase 10 comparisons.

Native Spark counts unique raw hashes: observed 87.9208%, scaled 2.2086%, complete
Phase 10 million run 2.68%. Stored/logical ratios about 4.73/0.97/0.77 include raw
and processed zones, schemas and file overhead: **not raw-only compression ratios**.
Cloning and entropy differences prevent extrapolation to production storage.

The measured Phase 10 reference profile (100 assets × 100 versions) has median
632-byte configs, 100 versions and 24-hour intervals; observed has 298-byte configs,
one version and roughly 126-day uncensored intervals. The 10k reference is labelled
separately from the original million-run evidence. Calibration changes key count,
diff/output mix, temporal range and reuse; measured throughput differences are not
a controlled single-factor experiment. High replay rates are accelerated history
replay/stress, not physical per-device frequency.

## Regression

| Command / suite | Passing cases |
| --- | ---: |
| `make check` | 744 |
| `make api-test` | 80 |
| `make spark-test` | 59 |
| `make integration-test` | 3 |
| `make spark-integration-test` | 3 |
| `make lake-integration-test` | 1 |
| `make search-integration-test` | 2 |
| `make api-integration-test` | 2 |
| `make phase10-integration-test` | 1 |
| `make phase11-integration-test` | 1 |
| **Distinct total** | **896** |

New cases: 33 offline importer/security/calibration unit cases, one local Spark
empirical transformation case and one real Kafka/Spark/MinIO fixture-history
integration case. Existing 861 cases remain. API suites retain the existing warning.
The final quality/API/Spark suites were rerun after report/plot changes. No existing
tests were weakened or deleted. Normal CI uses fixture Git histories only, no source
clone or service benchmark. Its existing quality/API/Spark jobs run these new cases.
The pushed commit's [GitHub Actions](https://github.com/kongwoang/ConfigStream/actions)
run and exact SHA are reported in the completion handoff, not guessed in advance.

Fixture end-to-end acceptance extracts three incarnations/seven revisions/four
adjacent diffs/one logging-disabled alert, preserves UTC and A-B-A identity, rejects
the fake suspicious marker before replay and validates historical aggregate totals.

## Executed workflows

The existing ignored local Docker/Lima and MinIO environment was sourced; no
credential file is printed or committed. Services were started using project targets.
Key data/replay/report commands (invoked with the project virtualenv) were:

```sh
make observed-fetch
make empirical-report
make observed-inspect
.venv/bin/python -m workloads.observed_main calibration-report --multiplier 40 --seed 42
.venv/bin/python -m workloads.observed_main generate --output .cache/workloads/observed-a.jsonl
.venv/bin/python -m workloads.observed_main generate --output .cache/workloads/observed-b.jsonl
.venv/bin/python -m workloads.observed_main benchmark --benchmark-id phase11-observed --rate 500
.venv/bin/python -m workloads.observed_main benchmark --benchmark-id phase11-calibrated-40 \
  --profile observed-calibrated --multiplier 40 --rate 1000
.venv/bin/python -m workloads.observed_main benchmark --benchmark-id phase11-calibrated-40-isolated \
  --profile observed-calibrated --multiplier 40 --rate 1000
.venv/bin/python -m workloads.observed_analysis \
  --runs phase11-observed phase11-calibrated-40-isolated main-1m-2g-50k-20261011
make empirical-report OBSERVED_ARGS='--summary-input workloads/results/real-history/observed-summary.json --results .cache/phase11/reproduced-report'
.venv/bin/python -m pytest tests/unit/test_observed_history.py
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
```

Fresh IDs are required to execute new replay runs; the IDs above identify actual
recorded runs and must not be reused. Extraction was repeated with the same aggregate
checksums, observed JSONL pairs compared byte-for-byte, and committed-summary plot
reproduction compared to generated artifacts. No million-run benchmark was repeated.

## Files and report artifacts

Created Python: `workloads/observed.py`, `workloads/empirical.py`,
`workloads/observed_main.py`, `workloads/observed_analysis.py`.
Created tests: `tests/conftest.py`, `tests/unit/test_observed_history.py`,
`tests/spark/test_observed_analytics.py`,
`tests/phase11_integration/test_observed_pipeline.py` and
`tests/fixtures/phase11/{history.json,README.md}`.
Created pinned specs: `workloads/manifests/real-history/{frr,nginx}.json`.
Created docs: this record, `real-history-sources.md`, `workload-calibration.md`,
`external-validity.md`, `related-work.md`.
Modified: `Makefile`, `pyproject.toml` (marker only), `benchmarks/main.py` (optional
history adapter), `README.md`, `docs/datasets.md`, `docs/benchmark.md`.
No dependency or lockfile change was needed.

Compact measured artifacts in `workloads/results/real-history/`:
`observed-summary.json`, `source-summary.csv`, `summary.md`, five CDF SVGs,
`manifests/{frr,nginx}.json`, `calibration/` (comparison JSON/CSV/Markdown, manifest,
five overlay SVGs), `replays/` (three compact actual results and summary CSV),
`replay-analytics.json` (typed output schemas, counts, timings and validation).
No raw configuration, cloned repository, generated JSONL history, log, credential,
checkpoint or service volume is staged.

## Decision and next phase

**Empirical validation improves, but natural Big Data volume evidence is weak.**
These public histories are sparse and small. Git is not a deployment sensor;
test/templates, first-parent traversal, censoring, scope/filter bias, rename boundaries,
tiny interval samples and bootstrap reuse limit external validity. Keep the topic,
but narrow it to **reproducible Big Data architecture evaluation using empirically
characterized configuration histories**. Do not claim a real million-device history,
naturally high-frequency config telemetry, production compression/throughput,
incident accuracy or horizontal scaling. Version retention alone is not novel.

Recommended next phase: consolidate report/defense, obtain consented larger observed
operational histories if available, pre-register matched size/key-count/reuse comparisons
and repeated controlled experiments. No dashboard, cloud, collectors or ML is implied.
Implementation stops after Phase 11.
