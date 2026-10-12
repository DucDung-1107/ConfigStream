# P17.1-2026-10-12 — bounded callback reuse and environment correction

Starting revision: `72705d6bd77e65e9aa3b6bf24c97063d5b03ae84`.
This is an additive execution decision, not a revision of Phase 13/17 measurements,
Phase 15's three RQs, authorization identity, oracle, or publication format.

## Environment discovery and owner authorization

PATH inspection initially found no Docker runtime. A deeper inspection found
the previously installed `.tools/docker/docker`, `.tools/lima/bin/limactl`,
`.cache/local-kafka-env.sh`, private `.cache/minio.env`, and stopped Lima instance
`k` under `.cache/l`. Those were already used by Phase 12. Thus Phase 17's
unavailable-service evidence remains historically correct but does not establish
that no recoverable local installation exists.

Starting the existing VM revealed only 7,063,482,368 free guest bytes on its
24 GiB disk. The team was asked about a bounded smoke-test exception. The owner
replied **“Cần thiết thì extend”**. Accordingly the stopped VM's disk was enlarged
to **64 GiB** using `limactl edit k --disk 64 --tty=false`, then restarted.
Its **2 CPUs and 6 GiB RAM are unchanged**. The guest subsequently reported
48,665,739,264 free bytes. No installation, volume deletion, image pull, RAM
increase or paid service occurred. Only existing Kafka and MinIO Compose services
were started, with `--no-build --pull never`; Elasticsearch was not started.
The 20 GiB host/guest reserve remains mandatory; no low-disk exception is used.

## Smallest optimization chosen

Baseline callbacks verify all earlier active-day raw segments, reconcile incoming
rows, publish a raw batch, then reread/rechecksum the same segments plus the new
batch before attempting completion. Profiling counts both invocations and Spark
task I/O, rather than attributing rising elapsed times to an unmeasured cause.

`reuse_verified_rows=True` materializes the already validated merged DataFrame
within **one callback**. Pending days reuse that evidence for a bounded readiness
count. Counts never authorize completion: a ready day still rereads and verifies
every committed physical raw segment, and runs the unchanged full source-map,
member-set, provenance, count and digest oracle barrier. Earlier coordinates are
still reconciled on every callback; conflicting retry fails before new publication.

The cache is disposable, not a persistent index. After interruption/restart,
published raw manifests are reread by the same committed-only reader. No sidecar,
new commit schema, metadata-only trust shortcut, migration or in-place rewrite is
introduced. Single-writer immutable-history assumptions remain explicit.

This reduces redundant work, **not its cumulative asymptotic order**. Earlier
segment verification, active-day reconciliation, oracle scans, sorted driver
digests and scheduling remain. A wider state engine/compactor is deliberately not
implemented during acceptance closure.

## Rollback and gates

`RpkiProcessor(..., reuse_verified_rows=False)` and
`workloads.rpki_main --processing-mode baseline` retain the prior processing path.
Both modes consume the same schemas and publish `rpki-1.0` metadata. Existing
configuration code, topics, paths and checkpoint formats are untouched.

The private profiling report bug and malformed negative-test construction are
retained in failure logs; they are not successful acceptance runs. No externally
sourced fixture was changed. Full remote acceptance requires the staged service
results in [closure acceptance](phase17-closure-acceptance.md), not this decision.
Execution limits were registered before service stages in
[execution policy](../workloads/results/phase17-closure/execution-policy.json).

## P17.1-2026-10-12-E-TIME-CAP

The first complete service day took 298.913 seconds. Extrapolated fourteen-day
planning time (not a measured result) was approximately 70–100 minutes, exceeding
the initial 60-minute stage cap. Before any Stage E attempt, the owner explicitly
approved **“Cho phép cap 120 phút cho run 14 ngày”**. Only the full frozen-window
overall cap becomes 7,200 seconds; each day's query cap stays 1,800 seconds.
All resource, oracle and stop guards remain. This is a preregistered, approved
planning amendment, not repeated timeout increases after failure. See
[budget amendment](../workloads/results/phase17-closure/budget-amendment.json).

## Execution revision and final safety hardening

The processing implementation is commit
`7722df0b5e9619394f0c03c1a070dd769f25ad39`. The full-window driver started from
that revision with the preregistered time-cap/reporting changes; its report pins
SHA-256 for the actual driver and source/processing/oracle modules. Earlier C/D
reports predate this hash-reporting enhancement and are identified by their
isolated query IDs rather than falsely attributed to a later whole-repository SHA.

Commit `a70d557e669a8066b95c5f6536423c69a5d2127c` also makes unavailable host
process-tree RSS samples fail closed, with two independent negative unit cases.
The long-running full-window process had already loaded the preceding sampler;
this error-path change is **not** claimed to have run inside that process. Final
resource acceptance therefore separately audits that every observed host RSS
sample exists, all filesystem/guest checks succeeded and all budgets were met.
Logical transformations, identity policies, oracle and publication semantics are
identical. A lost sample would leave the full-window resource gate unaccepted.
