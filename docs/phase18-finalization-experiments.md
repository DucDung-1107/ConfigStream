# Phase 18.2 — bounded tests and retained-history experiments

## Registered execution

The owner subsequently authorized reasonable resource use without further
questions. Use unchanged Spark `local[2]`/2-GiB heap and Lima 2-CPU/6-GiB/64-GiB
allocation. Register readonly five- and fifteen-minute checks at 600 seconds each,
and one retained full-hour recovery at 1,800 seconds after both pass. No source
events are republished. Any capacity/correctness failure stops enlargement.
Preserve 20 GiB host/guest free disk, 15 GiB growth/run, 50 GiB output cap and
512 MiB guest available RAM floor. Existing evidence remains retained.

Fresh service preflight found 36,126,334,976 guest free bytes and 5,618,319,360
available guest RAM before starting existing Kafka/MinIO. Metadata readback finds
151 `ris-1.1` commits, continuous batch IDs 0–150 and 604 Parquet objects /
894,628,418 bytes. Inventory SHA:
`d3d2f02fc82525d1c659f0b8136bc42ea64e6c33ca232050f59a3d1f1a70319a`.
Those facts establish a recovery candidate, not complete accepted logical history.

## Owned evidence

| Test | Measured result | Scope |
|---|---|---|
| Focused semantic/recovery suite | 44 passed, 30.50 s | Full-field conflict cases, missing/extra IDs, replay, cross-artifact multiplicity, interruption, metadata/file corruption |
| Medium profile | 100,000 identities + one retry, 16 partitions; 4.020 s | Owned comparison/disk persistence, no full-window claim |
| Profile heap / RSS | 2,147,483,648-byte max heap / 1,115,275,264 sampled process-tree peak | `local[2]` |
| Largest sorted tagged partition | 12,762 rows | Actual measured partition counts |
| Task metrics | 43,939,679 shuffle-written bytes; zero memory/disk spill; max task execution memory 34,340,848 bytes | All probe actions, including partition measurement |
| Actual service fixtures | 15 passed, 167.90 s | Existing 14 plus owned retained-finalization interruption/retry; no full-hour fault injection |
| Regression | 1,048 unit, 80 API, 99 Spark passed | Existing Terraform/RPKI compatibility included |

The earlier exploratory profile used the existing default `local[*]`; it is
retained privately and excluded from the registered `local[2]` capacity result.
Initial test authoring failures (two unchanged mutation values and tuple sorting
of nullable fields) and an import-name collision were corrected; the original
owned fixtures were not changed. One invocation without the opt-in environment
skipped all service cases and is not counted as service acceptance.

The old reconstructed conflict expression has four HashAggregate nodes. The new
tagged-sort input has zero, one Sort and explicit identity partitioning. Its
small finite status-count action still aggregates at most four keys. The probe's
cross joins construct owned inputs; they are not reconciliation joins. No matched
speedup claim is made and no spill is claimed where measured counters are zero.

## Commands and evidence policy

```sh
make check
make api-test
make spark-test
.venv/bin/python -m scripts.phase18_finalization_probe \
  --output .cache/phase18-finalization-probe-local2
RUN_PHASE18_SERVICES=1 .venv/bin/python -m pytest tests/phase18_integration -q
.venv/bin/python -m benchmarks.ris_service --finalize-only \
  --previous .cache/phase18-closure/service-hour-v2 \
  --approval .cache/phase18-finalization-hour-approval.json \
  --oracle .cache/phase18-closure/oracle-hour-v2 \
  --output .cache/phase18-finalization/service-hour-recovery
```

The last command requires the registered owner approval and prior acceptance
gates. A command is not evidence it ran. Source/normalized ledgers, service logs,
checkpoints and Spark task logs stay private. Public results contain compact
counts/checksums/timings only. Full-window outcomes are recorded separately after
execution in [acceptance](phase18-finalization-acceptance.md).

## Retained real history

The readonly first archive passed in 43.326 seconds with 370,141 exact rows and
unchanged SHA `ae6b5480b0ddb568c0b5efe466d84eeb11f8548c29a0c0f259473164ca3f4fbd`.
The readonly three-archive history passed in 100.342 seconds with 1,303,847 rows
and the original SHA `a2a0910bd20441aaf9a12c863c558b5f2b4c05198e932b95d49199fd79a151b3`.

The first full retained-history recovery failed after 271.528 seconds at ordered
digest caching (`MemoryStore.SerializedValuesHolder`, stage 1526), after exact
global comparison and twelve source-order checks passed. Checkpoint and immutable
batch inventory remained intact. No source/window completion was published.
This capacity finding is distinct from the old stage 4841 hash aggregation.

Change only ordered cache placement to DISK_ONLY, retaining partition-row guards,
ordering and SHA bytes. Twenty-eight focused Spark cases passed in 48.03 seconds;
the readonly fifteen-minute history again matched its original checksum in
97.974 seconds. Under the owner's reasonable-resource authorization, register a
new isolated recovery evidence directory at the same 1,800-second cap, same query
and immutable batch prefix. No source replay or heap increase occurs.

The corrected full recovery passed in **337.038 seconds**. Physical readback:
102.210 seconds; bounded reconciliation: 42.607; source verification: 97.384;
global projection SHA: 58.598; queries: 24.270; metadata publication: 3.873.
Timings are instrumented call boundaries, not pure operator CPU time. Task log
totals: 8,284 tasks, 6,639,241,276 shuffle-written bytes, zero reported spill,
105,119,696-byte maximum task execution memory. Disk caching is separate from
Spark spill counters. Failed-to-completed runs are not a matched speedup study.

Exact full logical projection now matches the independent twelve-source oracle.
All 57 frozen exclusions and twelve source descriptors are durably published;
the final window descriptor readback matches the recovery report. No native
events are republished and Kafka watermarks remain unchanged. The previous failed
source-ingestion attempt and negative cache experiment remain preserved.
