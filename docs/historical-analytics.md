# Historical Spark batch plane

`analytics/` reads only manifest-committed processed Parquet through `read_committed`.
It never reads Elasticsearch, raw Kafka bytes or configuration bodies. A frozen list
of input manifests is passed to the reader: concurrent new commits cannot change
the input halfway through a run. Exact typed payloads are deduplicated across input
queries, ignoring only delivery/location columns (ingest_time, query_id, batch_id,
year/month/day). A repeated snapshot/diff/alert ID with a different source payload
fails the run before any output publication. Distinct snapshot IDs claiming the
same asset/version also fail. Full normalized content and diff details participate
in reconciliation before they are dropped from analytics inputs.
Diagnostics count committed diagnostic occurrences, not inferred incident counts.

Run ID is SHA-256 of canonical sorted input manifests, `batch-v2` and parameters.
The identity-reconciliation change advances the job version so old `batch-v1`
results cannot bypass validation through a cache hit. Old results and evidence
remain untouched and are still visible to the metadata inspection command.
UTC timestamps and numeric thresholds are canonicalized: CLI integer defaults and
equivalent floating-point API values identify the same effective job.
New outputs are typed Snappy Parquet below
`analytics/<job>/publication_v2/run_id=<hash>/selection=<requested-job>`.
The request selection is `all` or the selected job name. Separate selections never
overwrite each other's committed files, including legacy paths.
`_analytics_commits/<hash>/<job>.json` is written only after every selected output
succeeds. Format `2.0` records each output's exact Parquet file inventory, sizes,
SHA-256 checksums, physical row count and schema. Repeating a completed run verifies
its identity, parameters, dataset paths and physical outputs before returning the
existing manifest without rewriting. Missing, extra or changed files fail closed.
Legacy format `1.0` at `analytics/<job>/run_id=<hash>` remains readable with physical
schema/count checks; it has no recorded checksums and cannot prove same-count
historical integrity. Legacy manifests are not upgraded or rewritten.
To rebuild a damaged result, use a new isolated lake prefix with verified inputs;
do not delete the marker and overwrite evidence under the original committed path.
Do not run concurrent writers with identical run/job IDs; there is no distributed
transaction or concurrent-writer lock. Uncommitted outputs are not authoritative.

## Jobs and output columns

| Job | Metadata-only result |
| --- | --- |
| change-frequency | asset/window, snapshot_count, diff_count, alert_count |
| fleet-activity | window, snapshot/diff/alert counts, distinct assets with diffs |
| diff-magnitude | count; mean, approximate p50/p90/p95, max for added/removed/changed |
| alert-trends | severity/rule/asset/window and count |
| diagnostic-trends | safe diagnostic type/window and count |
| asset-summary | asset, first/last snapshot time, counts, high/critical alerts, mean/max changed |
| anomalies | asset/window diff count, prior count/mean/stddev, z-score, flag/policy |
| fleet-bursts | window distinct changed assets and the same prior-only scores |

Windows are aligned to UTC (`1 hour` or `1 day`). Start is inclusive, end exclusive.
Snapshot counts include the observed baseline; a baseline is not itself a diff.
Diff counts count persisted comparisons, including zero-magnitude normalized
comparisons. Alerts are configured rule matches, not verified security incidents.
Diagnostics use event time, falling back to Kafka timestamp when available. Undated
diagnostics are retained in null-time buckets for an unfiltered run; explicit time
filters exclude them rather than inventing an event time. Anomaly baselines use
prior windows within the selected input interval, not history outside that interval.
Idle anomaly windows are explicitly filled with zero between each group's first
and last observed window. This calendar expansion can be expensive for large
fleets/long retention: restrict the query interval. No windows are invented outside
the observed period. A row outside this interval is not a measured maintenance period.

## Explainable anomaly formula

For each asset (or the fleet) at window T, use at most the prior 7 windows only:
`z = (count(T) - mean(prior)) / population_stddev(prior)`.
Flag if at least 3 prior windows exist, count >= 5, and z >= 3.
With zero variance, flag an excess above the constant baseline if count >= 5;
z-score is null (never infinity/division by zero). With fewer than 3 prior windows,
do not flag. The same defaults apply to fleet distinct-asset counts. This is a
historical explainable threshold, not ML, incident classification or an accuracy claim.
Future windows never enter a baseline. Thresholds are in `AnalyticsConfig`.

## Commands

Export local MinIO credentials from the ignored development file, then:

```sh
CONFIGSTREAM_LAKE_PREFIX=<isolated-history> SPARK_MASTER='local[2]' \
  make analytics-run ANALYTICS_ARGS='--query-id <checkpoint-query-uuid> --window "1 hour"'
CONFIGSTREAM_LAKE_PREFIX=<isolated-history> make analytics-inspect
make analytics-test
make phase10-integration-test
```

`--job` selects one job or `all`; `--start`/`--end` accept timezone-aware ISO times.
The reader validates committed paths before opening Parquet. Time filters bound
results, but there is no manifest/date-path pruning or independent partition/catalog
index. ID deduplication may also limit predicate pushdown; no pruning speedup is
claimed. Elasticsearch/API stats are small serving queries, not this batch plane.
