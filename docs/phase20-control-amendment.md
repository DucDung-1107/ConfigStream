# P20-CPU-CONTROL-AUDIT — 2026-10-14

The registered two-thread local budget was not explicitly applied to PyArrow's
native CPU/IO pools in the initial normalized pilot/E2R workers. SQLite queries
were single-threaded; actual CPU utilization/native concurrency was unmeasured.
Do not infer compliance merely from one-column processing.

Keep the same registered limit, explicitly set both Arrow pools to two, keep
SQLite one-threaded, preserve initial results as qualified evidence, and rerun all
affected bounded local configurations in fresh namespaces. No source, identity,
query, compression, preference threshold, VM/heap or runtime-cap change.
Initial exactness remains valid; initial timings are not final preference inputs.
This is a targeted benchmark-control repair, not a favorable post-hoc threshold
amendment or authorization for new full-window service ingestion.

[Decision](../workloads/results/phase20/control-amendment.json).

## P20-QUERY-BOUNDARY-AUDIT — same date

The first workers timed independent external-oracle comparison inside query cost.
This can mask an alternative's latency ratio. Preserve those values as qualified
and separate reconstruction/query/output materialization from exact independent
verification in the final matched repeats. Apply the unchanged 2x rule to the
candidate query only; do not skip any oracle comparison. Query parameters, source,
identity, compression and 20% threshold are unchanged. Command launch timing now
also records import/startup-inclusive completion separately.
This repairs a cost-boundary defect, not a favorable threshold change. Same
bounded local authorization/caps, no new service experiment or resource expansion.
[Decision](../workloads/results/phase20/query-boundary-amendment.json).
