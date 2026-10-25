# Architecture and dashboard integration review

Reviewed on 25 October 2026 against main revision `b5da3b3`, the supplied `ConfigStream_Architecture_Checkpoint_v6.tex`, and [PR #1 at 25d6b73](https://github.com/kongwoang/ConfigStream/pull/1/commits/25d6b73ff8776fc4fd5799771ceed0229a43f010).

Reference file SHA-256: `c56b7954f40c3a4d2d7580856d3b166d6d86a882949de35ab6bf384288278565`.

The supplied v6 file adds useful architecture detail. This integration preserves the original SimplePlus theme, English slides and Vietnamese notes. The technical walkthrough is now in the main presentation; only the Experiments and Operations dashboard screens remain in the appendix. The 26 October revision expands the main talk to about 18 minutes. PR #1 is used as a presentation reference; it is not merged wholesale. At the reviewed commit, the PR's dashboard addition is presentation content using screenshots already present on main, not new frontend functionality.

## Accuracy decisions

| Topic | Incorporated treatment | Code or evidence |
| --- | --- | --- |
| Component responsibilities | Source → Kafka → Spark → MinIO; search/API, alert and batch branches are shown separately. | `spark/streaming/outputs.py`, `analytics/main.py`, `search/reindex.py` |
| Publication order | In the lake-enabled path, Parquet write and verification precede manifest publication; search indexing and Kafka alerts follow. Spark checkpoint progress follows callback return. This is not an atomic transaction across sinks. | `spark/streaming/sinks/lake.py`, `spark/streaming/outputs.py` |
| Retry behavior | An existing committed batch is verified and reused only for an exact retry. The comparison includes row multiplicity. Physical alert duplicates remain possible after a later failure. | `spark/streaming/sinks/lake.py` |
| Two stateful stages | Parse/validate first, then identity state keyed by `snapshot_id`, then version state keyed by `asset_id`. Per-asset processing normalizes before adding to the version buffer. | `spark/streaming/validation.py`, `identity.py`, `stateful.py`, `processing/state.py` |
| Kafka semantics | Offsets locate records in a partition; source version numbers determine adjacent configuration comparisons. No cross-topic total order or atomic publication is promised. | `messaging/kafka_producer.py`, `spark/streaming/kafka_source.py`, `docs/kafka-topics.md` |
| Raw and processed history | Raw fetched Kafka key/value bytes and provenance use a separate archival branch. Processed configuration output uses typed, date-partitioned paths and commit manifests. Other domains have their own layouts. | `spark/streaming/sinks/lake.py`, `docs/data-lake.md` |
| DNS comparison | Do not import a claim that observations are verified to share a measurement vantage point. Grouping uses query/response fields within the selected source; worker/time provenance and unknown scope remain relevant. | `spark/dns.py` |
| History UI | Asset/date filters and versions/diffs/alerts expose safe configuration metadata. The shown fixture has 2 versions and 1 diff; TerraDS aggregate counts do not imply all TerraDS history is served by this API. | `docs/phase20v-demo-script.md`, `docs/screenshots/phase20v/history-desktop.png` |
| Experiments UI | Recorded RPKI measurements and their declared scope explain the charts; opening the page does not execute a benchmark. | `frontend/public/research/v1/catalog.json`, `docs/screenshots/phase20v/experiments-desktop.png` |
| Operations UI | Node, lag and resource cards are recorded evidence. “Check API now” makes a fresh readiness request when the backend is available. READY in the screenshot refers only to its displayed timestamp. | `docs/phase20v-data-contracts.md`, `docs/screenshots/phase20v/operations-desktop.png` |

## Evidence boundaries

The existing replay and single-node deployment receipts establish their recorded scopes. They do not validate a newly built image automatically. The current configuration integrity changes still need a completed Kubernetes recovery rerun; the slide explicitly retains that limitation. Neither the architecture diagram nor a dashboard screenshot establishes scalability, high availability or current backend availability.

The three additional dashboard captures are historical Phase 20V images from 14 October 2026. The main overview capture remains the actual frontend capture from 25 October 2026. Screenshot bytes are preserved; cropping is specified in LaTeX.

## Technical expansion — 26 October 2026

The initial integration placed too much implementation detail in the appendix. The revised main section follows the v6 architecture narrative while retaining the corrections above:

- A snapshot field table distinguishes source identity, version, event time, ingest time and content integrity; values are explicitly illustrative.
- A Kafka offset/version example shows why delivery order cannot select the predecessor.
- Spark's snapshot-ID and asset-ID state operators are explained separately, including source binding, retry/conflict behavior, emitted-edge flags, timers and retention.
- A four-row execution trace shows the buffer/output for v1, v3, v2 and a retry. The diff slide includes the actual telnet YAML rule.
- The lake slide explains raw versus processed data, query/batch namespaces and the different meanings of Kafka, Spark and date partitions.
- Commit verification is followed by a failure-point table, distinguishing lake manifests from Spark checkpoints and logical deduplication from physical alert delivery.
- A new editable `history-readers` diagram separates historical batch output from Elasticsearch/API serving; its dashed edge denotes an explicit index rebuild.

The root README now leads with the detailed configuration architecture and places the dashboard below results. The four-domain diagram remains linked to make the configuration diagram's scope explicit.

## Validation

- Both presentation and presenter PDFs compile to 23 pages: 21 main slides and 2 appendix slides, with separate numbering.
- New diagrams have editable draw.io sources and validated exports.
- Slide crops, architecture diagrams and the presenter layout were rendered and inspected.
- Vietnamese Markdown notes and both contact sheets are regenerated from the final source/PDFs using `export-previews.py`.
- This is a documentation change. No new integrated replay or benchmark is claimed.
