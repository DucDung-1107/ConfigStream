# Historical Spark batch plane

`analytics/` reads only manifest-committed processed Parquet through `read_committed`.
It never reads Elasticsearch, raw Kafka bytes or configuration bodies. A frozen list
of input manifests is passed to the reader: concurrent new commits cannot change
the input halfway through a run. Domain IDs are deduplicated across input queries.
Diagnostics count committed diagnostic occurrences, not inferred incident counts.

Run ID is SHA-256 of canonical sorted input manifests, `batch-v1` and parameters.
Outputs are typed Snappy Parquet below `analytics/<job>/run_id=<hash>`.
`_analytics_commits/<hash>/<job>.json` is written only after every selected output
succeeds. Repeating a completed run returns its existing manifest without rewriting.
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
The reader validates committed paths before opening Parquet. Timestamp predicates
can be pushed into Parquet, but commit discovery currently examines all manifests;
there is no independent partition/catalog index. Elasticsearch/API stats remain
small serving aggregations, not this historical batch plane.
