# Phase 20 — common correctness campaign

See [scope matrix](../workloads/results/phase20/correctness-matrix.json),
[recovery evidence](../workloads/results/phase20/recovery-results.json) and
[invariant coverage](../workloads/results/phase20/invariant-coverage.json).

Current service-free regression reruns typed contracts, Terraform/RPKI/RIS/DNS
identity/reconstruction/uncertainty tests. Fourteen focused evaluation cases check
owned fourteen-day full/delta/checkpoint equivalence, missing/nonexistent deltas,
separated query/verification timing and unchanged fixed checksum vectors,
exact missing/extra/changed tuples, preference thresholds, missing required query,
failed resource sampling and falsification of unsafe payload dedup.

The unsafe control collapses two distinct source coordinates with equal payload
into one. Correct identity preservation retains two: B8 is demonstrably wrong,
never a production path. Existing domain fixtures cover retry/conflict, disorder,
ties, missing/partial sources, invalid policy/provenance, unsupported parser records,
delayed completion, commit/checkpoint failures and corrupt committed evidence.
Each remains subject to its original evidence scope, not a newly claimed real fault.

H1 is supported within those declared tests and previously accepted windows.
New actual Kafka/Spark/MinIO fault tests: **0**. Reuse prior RPKI 9, RIS 15 and DNS
14 phase-local fixture results separately; do not sum overlapping historical
campaigns or call them new Phase20 acceptance.

I1 provenance, I2 replay, I3 multiplicity, I4 scoped completeness, I5 honest time,
I6 committed visibility, I7 exact projection, I8 accounting and I9 compatibility
remain distinct. Exact E2R row comparisons back digests. Missing RPKI source cannot
derive removal; missing/timeout DNS is not empty/deletion; RIS equal fingerprints
are separate occurrences; Terraform orders by accepted ordinals, not deployments.

Recovery metrics retain full/partial timing labels. RIS's successful 337.038-s
retained finalization republishes no events and changes no checkpoint. DNS's
39.850-s 256-row retry adds no logical rows. These are not uniform replay targets,
isolated restart timings, power-loss durability or cross-system transactions.
