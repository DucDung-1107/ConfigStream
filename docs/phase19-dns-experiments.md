# Phase 19 — Staged experiments

Inputs are exactly the two private pinned `.li` zonefile-basis objects, not a
complete daily partition or authoritative zone. Source total: 98,828,670 bytes.
See [source attribution and terms](phase19-source-attribution.md).

## Registered execution

Owner authorized reasonable resource use without further questions. The
[DNS-specific policy](../workloads/results/phase19/entry-and-execution-policy.json)
registers oracle 1,200 s, service sample 600 s, one object 1,800 s, both objects
3,600 s. No cap inflation or automatic lengthy retry, downloads or VM expansion.
Spark remains `local[2]`, 2-GiB driver heap; Lima remains two CPUs / 6-GiB RAM /
64-GiB disk. Preserve 20-GiB host/guest free disk, 15-GiB per-run growth,
50-GiB new-output budget and 512-MiB available guest RAM. Fail-closed samplers
and cancellation protect each run. Each has a fresh UUID/topic/checkpoint/prefix.
Idle-sleep prevention is scoped to the running command.

## Completed reference and initial stages

Independent two-object oracle: 302.21 s wall, maximum process RSS 1,048,477,696
bytes; output approximately 3.1 GiB including SQLite and validation projections.
It is not a fair direct speed comparison with streaming.

The first 4,096-row service sample failed after exact readback because Spark 4
represented its offset as a dictionary rather than a JSON string. The evidence
was preserved; both forms now have explicit tests. A fresh bounded sample passed
in 19.288 s, 670,376 Parquet bytes, lag zero, RSS 1,466,515,456 bytes. It has no
source-completion descriptor and does not prove whole-object coverage.

September 1 complete object passed in 595.181 s: exact 1,117,660 rows,
176,790 selected addresses, 145,559 groups, contributor/value-reuse reconstruction,
245,960,651 Parquet bytes in 78 objects, lag zero, RSS 2,565,390,336 bytes.
The heap remains 2 GiB; sampled process-tree RSS also includes Python/JVM native
memory and must not be interpreted as driver heap consumption.

Final two-object, representation and recovery measurements are recorded in
[machine-readable validation](../workloads/results/phase19/validation.json)
and [acceptance](phase19-acceptance.md). Only completed runs are accepted.

Both selected objects passed in 1,224.086 s, with 538,543,164 Parquet bytes and
peak sampled process-tree RSS 2,742,534,144 bytes. Exact independent comparison
and both source barriers passed; final lag was zero. A separate 39.850-s replay
of 256 real rows preserved the raw inventory and completed remote reconstruction
and the additive selected-window binding. This is not another full ingestion run.

The value-reuse components are **2,207,468 bytes larger** than the full-group
components under the same shared raw/contributor projection. Preserve this
negative finding. Whole prototype storage includes both alternatives, validation
metadata and differences; no overall compression or streaming superiority claim.

## Reproduction

```sh
make check
make api-test
make spark-test
# Optional dependencies for the full local validation environment:
.tools/uv/bin/uv sync --locked --all-extras
# Existing local service environment and private credentials only:
source .cache/local-kafka-env.sh
set -a; source .cache/minio.env; set +a
RUN_PHASE19_SERVICES=1 .venv/bin/python -m pytest tests/phase19_integration
caffeinate -i .venv/bin/python -m benchmarks.dns_service --stage sample --output .cache/phase19/sample-new
caffeinate -i .venv/bin/python -m benchmarks.dns_service --stage one --output .cache/phase19/one-new
caffeinate -i .venv/bin/python -m benchmarks.dns_service --stage two --output .cache/phase19/two-new
# Only after that run passes:
caffeinate -i .venv/bin/python -m benchmarks.dns_recovery --accepted .cache/phase19/two-new --output .cache/phase19/replay-new
```

These are opt-in controlled workflows, not automatic replay instructions. Advance
only after the prior stage passes and current guards/rights hold. CI uses owned
fixtures only. No real source download is required. DNS replay rows/s is not a
natural DNS configuration-change rate or the RIS source-event rate. Full/reused
group representations include their common raw/provenance/contributor obligations;
smaller value bytes alone do not establish overall storage superiority.
