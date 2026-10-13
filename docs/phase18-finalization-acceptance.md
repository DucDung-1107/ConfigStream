# Phase 18.2 — acceptance record

**PASS — FULL-HOUR RIS ACCEPTANCE CLOSED BY RETAINED-HISTORY RECOVERY**.
Phase 18/18.1 results remain unchanged. Their failed runs are still failed.

The retained 151 batches have now been independently established as the exact
complete logical EventRow projection under the pinned policy: **4,725,156**
distinct occurrences, zero missing/unexpected/conflicting identities, **118,172**
legitimate repeats, twelve source-accounting descriptors and **57** durable
exclusions. Complete-window readback and historical queries pass. Projection SHA:
`fc2fdc58bdd4801916a23ead06bef60ff2609a4ad2bc58358b2172850d118450`.

Readonly five-/fifteen-minute checks passed in 43.326 / 100.342 seconds with the
original checksums. A first recovery failed after 271.528 seconds at the ordered
digest's memory cache, after exact comparison and all twelve source checks passed.
The reviewed DISK_ONLY correction preserved the fifteen-minute checksum in a
97.974-second readonly check. The new same-cap recovery **passed in 337.038
seconds**, with unchanged 2-GiB heap, VM and 1,800-second cap. No events were
republished, no Parquet or batch metadata rewritten, and the checkpoint fingerprint
remained unchanged. Failed output and earlier failures are retained.

I1/I3/I4/I5/I7/I8 have selected real-window evidence; I2/I6 retain actual owned
replay/corruption fixture evidence, and I9 has regression evidence. These do not
establish power-loss, failover, multiwriter transactions, universal BGP validity
or routing-table reconstruction. Instructor approval and unrestricted source
redistribution remain unverified; raw/normalized records stay private with RIPE
RIS attribution. The owner's subsequent explicit Phase 19 request may enter its
source-specific rights/semantic gates with **GO — PHASE 18.2 ACCEPTED**.

Peak sampled recovery RSS: **2,224,193,536 bytes**; guest free disk remained above
36,119,662,592 bytes and available RAM above 4,468,011,008 bytes. No sampler guard
failed. Parquet stays 604 objects / 894,628,418 bytes; final prefix is 896,868,378
bytes, including 37,178 bytes of source/exclusion sidecars. Kafka watermarks
remain 12,223 / 24,699; no new source chunks were published.

Regression: **1,048 unit, 80 API, 99 Spark**; **15 actual-service owned fixture
tests**. Digest correction: 28 focused Spark cases. Execution used
`496e48f7771dad28c49386fceb452b16e956b490`, whose
[CI passed](https://github.com/kongwoang/ConfigStream/actions/runs/38426323262).
Normal CI does not run the real workload. Final evidence delivery CI is reported
at handoff. [Machine acceptance](../workloads/results/phase18-finalization/acceptance.json)
and [complete recovery](../workloads/results/phase18-finalization/full-hour-validation.json)
provide exact evidence scopes.

Starting SHA: `f5b62bb947e6dfa4626687f08ff2245b928b34b1`. Final implementation,
execution and delivery SHAs plus exact CI links are recorded in the handoff and
machine-readable artifacts.

Reports: [root cause](phase18-finalization-root-cause.md),
[correctness](phase18-finalization-correctness.md),
[experiments](phase18-finalization-experiments.md).
