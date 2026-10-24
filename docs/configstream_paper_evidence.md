# Evidence ledger for `configstream_paper.md`

This ledger records the local source used for the paper's empirical claims. It is a drafting aid, not a substitute for human verification before submission.

| ID | Claim or number | Local source | Locator / note |
|---|---|---|---|
| E01 | ConfigStream stores, compares and analyses configuration changes over time; the asset is the central entity. | `README.md` | Opening description and architecture overview |
| E02 | Kafka → Spark → MinIO/Parquet → Elasticsearch/FastAPI is the implemented pipeline boundary. | `README.md`, `docs/architecture.md` | Pipeline diagrams and component responsibilities |
| E03 | Pairing uses `asset_id` and adjacent versions, not arrival order. | `docs/stateful-processing.md`, `docs/config-processing.md` | Implemented topology and pair-selection policy |
| E04 | Normalization is deterministic, conservative, line-oriented, and not a universal secret detector. | `docs/config-processing.md` | Exact normalization policy and limits |
| E05 | TerraDS accepted history contains 6,505 incarnations, 10,188 versions, 3,683 pairs, and 12,278,085 bytes. | `docs/phase15-research-methodology.md`, `workloads/results/terrads/history-summary.json` | Dataset-role matrix and history summary |
| E06 | RPKI, DNS and RIS retain distinct semantics and units. | `docs/phase15-research-methodology.md` | Table A and three-question evaluation design |
| E07 | TerraDS replay accepted 10,188 snapshots and 3,683 pairs with 29,402,997 committed bytes, zero alerts/diagnostics, publisher 500.023/s and 68.742 s completion. | `docs/benchmark.md`, `docs/phase12-acceptance.md` | Phase 12 real IaC replay |
| E08 | Corrected matched campaign has 27 trials, three repetitions for nine configurations, and exact row-level matching. | `docs/phase20-processing.md`, `workloads/results/phase20/matched-processing-repetitions.json` | Corrected pilot section and repetitions artifact |
| E09 | Matched medians are RPKI 6.297/16.840/25.417 s, RIS 10.485/21.167/29.644 s, and DNS 2.089/10.536/16.866 s for B0/B2/B3. | `docs/phase20-processing.md` | Corrected, separately approved pilot table |
| E10 | H3 and full paced RIS comparison were not run. | `docs/phase20-processing.md`, `docs/phase20-acceptance.md` | Pending/deferred approval boundary |
| E11 | Matched campaign storage/resource observations are 1,622,766,994 Parquet bytes, 1,623,023,143 MinIO bytes, 3,869,485,576 bytes retained growth, and no resource failure. | `docs/phase20-processing.md` | Corrected pilot resource section |
| E12 | Local/single-node acceptance does not establish HA, distributed scalability, or exactly-once end-to-end. | `README.md`, `docs/architecture.md`, `docs/benchmark.md` | Current status and limitations |
| E13 | Structured Streaming, Dataflow, Kafka, MapReduce, RDD, Delta Lake and DuckDB bibliographic metadata. | `docs/related-work.md` plus verified primary sources listed in `configstream_references.bib` | Citation metadata checked with citation-management script and web sources |

## Unresolved items before submission

1. Human authors should open every primary reference and check the final ACL bibliography formatting against the target venue.
2. Human authors should re-run the evidence artifacts from a clean checkout if the paper is submitted as a reproducibility claim.
3. The exact benchmark hardware/OS for every reported run should be copied from the corresponding acceptance receipt into the final Methods section if the venue requires machine-level reproducibility.
4. Author contributions, funding, conflict-of-interest, code/data availability, and AI-use disclosure require author approval; they are intentionally not invented here.
