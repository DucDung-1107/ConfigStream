# Curated observed configuration history

**Git history is an observable public proxy for configuration evolution.** It is
not a measurement of router deployments, running state or per-device frequency.
Selection and retrieval were recorded on 2026-10-11. No broad GitHub crawl was used.

## Candidate decisions

| Candidate | Ownership/purpose | License evidence | Decision |
| --- | --- | --- | --- |
| [FRRouting/frr](https://github.com/FRRouting/frr) | Official routing project; BGP/OSPF topology-test configurations | [Pinned upstream documentation](https://github.com/FRRouting/frr/blob/c4e94c4aaa4f4562313b4a168f91211afc3bcba2/doc/developer/topotests.rst) declares topotest configurations/scripts ISC-style; representative test script has SPDX ISC. Repository as a whole has mixed licenses | Accepted for observed **test configuration** evolution, not device deployment history |
| [nginx/nginx](https://github.com/nginx/nginx) | Official web-server project; shipped configuration templates/parameters | [Pinned LICENSE](https://github.com/nginx/nginx/blob/2b5c2b605b5df669da5dec6749dcc76c07d1315d/LICENSE), BSD-2-Clause | Accepted as a sparse application-domain counterexample; not rejected merely because it has few configuration changes |
| [kubernetes/examples](https://github.com/kubernetes/examples) | Official curated infrastructure examples | Repository metadata reports Apache-2.0 | Deferred/rejected for this extraction round: would add another template corpus but not deployment evidence; two explicit domains already selected. No clone/history statistics or credential-risk conclusion was made |
| Existing [batfish/batfish](https://github.com/batfish/batfish) selection | Public parser/lab baselines, already measured in Phase 10 | Pinned Apache-2.0 source | Retained as baseline evidence, not relabelled as observed history. A second network parser-test history extraction was not selected |

The additional candidates were considered, not secretly filtered until distributions
matched synthetic assumptions. FRRouting and NGINX both show much sparser histories
than the Phase 10 controlled profile. No private enterprise history is represented.

## Exact extraction boundaries

Both repositories' default branch was verified as `master`. Extraction never uses
moving HEAD after pinning. The first-parent start/end revisions are inclusive:

| Source | Start SHA | End SHA | Scope/patterns |
| --- | --- | --- | --- |
| FRRouting | `01dbc4b3843b836b7be90d22d0276f55c4001f42` | `c4e94c4aaa4f4562313b4a168f91211afc3bcba2` | `tests/topotests/bgp*/*.conf`, `tests/topotests/ospf*/*.conf` |
| NGINX | `dc1b14126e0a7a45018d95b149ebdb29985f18f1` | `2b5c2b605b5df669da5dec6749dcc76c07d1315d` | `conf/*.conf`, `conf/*_params`, `conf/mime.types` |

| Measured extraction | FRRouting | NGINX |
| --- | ---: | ---: |
| First-parent commits enumerated/inspected for scope | 4553 | 1049 |
| Commits affecting broad directory scope after baseline | 1358 | 4 |
| Candidate distinct configuration paths | 2115 | 6 |
| Accepted distinct paths | 1876 | 6 |
| File-incarnation assets | 1900 | 6 |
| Accepted file revisions | 2516 | 9 |
| Unique content SHA-256 values | 2211 | 9 |
| Revision-content bytes | 1,109,458 | 24,540 |
| Rejected whole paths | 239 | 0 |
| Potential credential keyword/long-secret rejection | 200 | 0 |
| Empty/oversized revision rejection | 39 | 0 |

Counts are generated from pinned Git objects, not repository search-result estimates.
Scope commits may change scripts without changing selected configurations. 24 extra
FRRouting assets are path deletion/recreation incarnations, not additional natural
devices. Measured manifests/checksums are in `workloads/results/real-history/manifests/`;
fetch specifications are in `workloads/manifests/real-history/`.

Repository boundary times are FRRouting 2023-01-11 to 2026-10-09 UTC and NGINX
2020-09-09 to 2026-09-30 UTC. Snapshot span can end earlier than repository history:
the last selected NGINX configuration revision is in 2025, not the last code commit.
First-parent shallow boundaries were inspected and then pinned; the fetch cutoff
alone is not asserted to be the exact oldest extracted revision.

## Extraction/provenance contract

`workloads.observed` verifies exact Git revisions, first-parent membership, origin
allowlist and SHA-256 of upstream license evidence. Fetches are time-focused bare
repositories under ignored `.cache/datasets/git/`; no checkout or external hooks
are needed. Git objects, external licenses and raw bodies are not committed here.

Initial selected files are boundary baselines. Subsequent identical blobs are
suppressed. Deleted/recreated paths become independent assets; renames are treated
as deletion plus creation, **not followed**. This conservative boundary avoids fake
cross-path diffs, but loses real continuity and can underestimate versions/file.
Each accepted incarnation is ordered by committer timestamp, then first-parent
ordinal for ties. No timestamp-order inversions were measured in these selections;
clock skew would be reported rather than silently changing timestamps.

Event time uses Git **committer** `%cI`, retaining the original offset in provenance
and converting the snapshot to UTC. Commits bundled through merges appear at the
first-parent merge point, not as separately replayed side-branch deployments.
Version numbers starting at 1 are ConfigStream-assigned extraction ordinals.
Deterministic ingest time is event time + 5 ms, a replay convention, not an observed
historical collection delay. Baseline timestamps are left-censored boundaries,
not claims that a configuration was originally created at that boundary.

Ignored output per source: `histories.jsonl` (existing ConfigSnapshot),
`provenance.jsonl`, `manifest.json`, `LICENSE.source`. Provenance includes repository,
path, Git commit/previous selected commit, original timestamp, UTC event time,
content SHA-256, extracted version, asset type, source manifest ID/license,
boundary-baseline flag, commit ordinal and observation end. Aggregate SHA-256 binds
provenance; loading revalidates source-content hash, identity/time and security.

## Security filtering

Only the two explicit repositories are fetchable. Non-UTF8, empty/oversized,
private-key markers, password/token/API/SNMP/authentication keywords, bearer tokens,
cloud-key patterns and suspicious 64-character secret-like strings are rejected.
Any failing candidate revision rejects the **whole path history**, including earlier
safe revisions: no fabricated diff bridges a rejected revision. Only category/count
metadata is published. Raw source bodies/rejected paths are not logged.

This is deliberately overinclusive and not a universal detector. 200 rejections
do not mean 200 real credentials were found. Filtering, test-only contents and
scope/boundary choices introduce selection bias. Config bodies remain in the ignored
dataset/restricted lake; Elasticsearch/FastAPI remain metadata-only and unchanged.

## Reproducible commands

```sh
make observed-fetch
make empirical-report
make observed-inspect
python -m workloads.observed_main generate --output .cache/workloads/observed.jsonl
make observed-replay OBSERVED_ARGS='--rate 500'
make calibration-generate OBSERVED_ARGS='--multiplier 40 --seed 42'
python -m workloads.observed_main calibration-report --multiplier 40 --seed 42
make phase11-integration-test
```

Replay publishes Asset once then ConfigSnapshot through the existing producer;
Spark, not the importer, derives diffs/rules. JSONL generation also emits ChangeEvent
and a source-snapshot provenance sidecar. Physical rate is accelerated replay, not
the original logical timing. Fresh benchmark/checkpoint/lake IDs are mandatory.
Full sources are not fetched by setup or CI. Offline fixtures cover unchanged commits,
UTC, rename boundaries, A-B-A reversion, seeded scaling and security rejection.

For standalone `observed-replay`, start a fresh lake-enabled Spark query with
`SPARK_WATERMARK_DELAY='1000000 hours'`, `SPARK_STATE_TIMEOUT='1000000 hours'`
and a new checkpoint/lake prefix **before publishing**. These replay-specific settings
retain the multi-year history; ordinary short-retention streaming settings can expire
state between sparse observed versions. The benchmark adapter starts/configures this
consumer automatically and isolates beginning Kafka offsets. Neither command changes
normal production/demo defaults.

To reproduce committed plots without downloading repositories:

```sh
python -m workloads.observed_main report \
  --summary-input workloads/results/real-history/observed-summary.json \
  --results .cache/phase11/reproduced-report
```
