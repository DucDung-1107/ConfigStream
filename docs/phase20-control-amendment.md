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
