# Phase 20 — processing costs and pending matched engines

[Registered cells](../workloads/results/phase20/evaluation-matrix.json) and
[pilot trials](../workloads/results/phase20/pilot-results.json) distinguish
normalized direct-Parquet work from raw parsing and real transport.

Three trials each re-encode one complete RPKI day (360,226 members), first complete
RIS five-minute accepted projection (370,141 occurrences) and first 4096 DNS raw
measurements. Every full Arrow projection compares exactly. Same Snappy/65536-row
target; no native events or statuses are discarded. Startup/module-import time
is NOT_MEASURED; worker elapsed begins after imports. Parser construction is
EXCLUDED. No Kafka messages or Spark callbacks were produced by this pilot.

Thus B0/B2/B3 full-window matched engine comparisons are **NOT_RUN**. Existing
RPKI 5,053.741-s full service run and DNS 1,224.086-s run include different parsing,
publication, verification and recovery work. Do not compare the pilot's direct
rewrite time or oracle time against them as an engine ranking. The prior RIS
full-hour ingestion failed at finalization and was later recovered; it is not a
new successful clean ingestion benchmark.

H3 is **NOT_RUN**: no 0.5/1/2x paced replay was started. Required 1x rule remains
exact logical IDs, zero selected-offset lag, resource compliance and
post-producer-to-final-validated-commit drain <=10% of producer duration.
Accepted rate includes stream-ready through final validated commit. Do not hide
validation time or substitute historical 1,312.543333 source occurrences/s.

[Budget proposal](../workloads/results/phase20/budget-proposal.json) uses ideal
uniform target arithmetic: full nested 3-rate/2-pacing/3-repeat producer work alone
is approximately **28.44 hours**, excluding startup/drain/validation; burst pacing
may differ. Old raw-parsing times are labeled unmatched estimates, not completion
promises. Current guest headroom above reserve is about 10 GiB; complete retained
matrix growth is not established/approved. No VM resize, deletes or full-hour
replay. Request separate owner approval for matched bounded service pilots first.

All per-run measurements, slower runs and the initial virtualenv bootstrap failure
remain recorded. The failed namespace is not renamed successful or averaged.
CPU, physical query I/O and network wire bytes are NOT_MEASURED, not zero.

```sh
source .cache/local-kafka-env.sh
caffeinate -i .venv/bin/python -m benchmarks.phase20 --suite pilot --output .cache/phase20/pilot-new
```
This command is normalized local-only, not authorization for E3 service expansion.
