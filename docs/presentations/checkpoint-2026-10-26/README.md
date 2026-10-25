# Group 4 — first milestone checkpoint

15 main English slides and 5 appendix slides for 26 October 2026, with Vietnamese speaker notes. Planned main speaking time: approximately 12 minutes (12:10 in the notes).

- [Presentation PDF](main.pdf): use this when projecting.
- [Presenter PDF](speaker-notes.pdf): each page has the slide on the left and Vietnamese notes on the right.
- [Speaker notes](speaker-notes.md): convenient rehearsal copy.
- [Overview preview](preview.png): the 15 main slides.
- [Appendix preview](appendix-preview.png): 5 slides for questions.
- [Architecture integration review](architecture-integration.md): accuracy checks against the v6 reference and PR #1.
- [LaTeX source](main.tex): edit slide text and `\vnote{...}` here.

The deck introduces the problem, demonstrates version ordering, introduces all four data types (Terraform, RPKI, RIS and DNS), explains their separate processing paths, and maps implemented progress to the three checkpoint requirements: executable code in Git, prepared data, and system architecture.

## Template and illustrations

The four SimplePlus theme files are copied unchanged from the supplied template, preserved in [../SimplePlus_Blank_Template](../SimplePlus_Blank_Template). The main architecture figure `four-paths` shows all four domain pipelines. The new `config-path`, `commit-order` and `spark-stages` figures explain component roles, publication order and the two stateful stages. Older illustration assets remain available for reuse. All architecture, version-order, workflow and replay diagrams have editable `.drawio` sources beside their exported PNGs in `figs/`.

`figs/dashboard.png` is a cropped capture of the actual local frontend on 25 October 2026. Its cards and chart display the accepted static research evidence bundle; it is not a live backend replay. Capture used Chromium, a 1440 × 1000 viewport, device scale factor 1.5, and clip `{x: 300, y: 70, width: 1110, height: 660}`. No application data was mocked for the screenshot.

The History, Experiments and Operations screenshots are copied unchanged from `docs/screenshots/phase20v/`, recorded on 14 October 2026. LaTeX crops focus on relevant controls and tables. History needs the configuration API when used interactively; research pages use the accepted evidence bundle. Operations combines recorded metrics with an explicitly requested readiness probe. The historical READY label is not a current health claim. All images used by the deck are inside `figs/`.

## Build

Requires a LaTeX distribution with Beamer, `appendixnumberbeamer`, Vietnamese T5 fonts and `latexmk` (tested with TeX Live 2026):

```sh
cd docs/presentations/checkpoint-2026-10-26
sh build.sh
```

On macOS with MacTeX, if the tools are not in `PATH`:

```sh
PATH=/Library/TeX/texbin:$PATH sh build.sh
```

The script builds both PDFs in `_build/` and copies the final files to this directory. The LaTeX sources, four theme files and `figs/` can also be uploaded together to Overleaf; choose `main.tex` and pdfLaTeX. Choose `speaker-notes.tex` for the presenter version. After building, synchronize Markdown notes and both contact sheets with:

```sh
# Requires PyMuPDF in the active Python environment
python export-previews.py
```

The exporter checks PDF page counts against the LaTeX source. Main slides and appendix slides have separate page numbering.

## Evidence and scope

The deck distinguishes current executable code from previously recorded integrated runs:

| Claim | Repository source |
| --- | --- |
| Demonstration input and normalized diff | `processing/demo.py`, `processing/diff.py` |
| Severity of logging / telnet rules | `rules/default.yaml`, `tests/unit/test_rules.py` |
| Late-arrival handling with bounded retained state | `processing/state.py`, `spark/streaming/stateful.py` |
| Configuration storage / serving path | `spark/streaming/outputs.py`, `spark/streaming/sinks/lake.py`, `search/reindex.py`, `analytics/main.py`, `api/`, `frontend/` |
| Snapshot identity before per-asset state | `spark/streaming/identity.py`, `spark/streaming/stateful.py`, `processing/state.py` |
| Recorded dashboard captures and live/static boundaries | `docs/phase20v-demo-script.md`, `docs/phase20v-data-contracts.md`, `docs/screenshots/phase20v/` |
| 6,505 file histories; 10,188 accepted versions; 3,683 adjacent pairs | `workloads/results/terrads/history-summary.json` |
| Recorded local replay and 29,402,997 Parquet bytes | `docs/phase12-acceptance.md` |
| Accepted RPKI / RIS / DNS counts and scopes; dashboard evidence bundle | `frontend/public/research/v1/catalog.json` |

Rechecked on 25 October 2026:

```sh
make processing-demo
.venv/bin/python -m pytest tests/unit/test_state_logic.py tests/unit/test_diff.py tests/unit/test_rules.py -q --tb=short
# 178 passed
```

These commands verify the small Python processing path. The historical full Kafka/Spark replay was not rerun while preparing the slides. The current integrity implementation is described from code; a completed Kubernetes recovery rerun for the latest image remains outstanding. Git revisions are not deployment events. Recorded local / single-node evidence does not establish distributed scalability or high availability.

Before presenting, prepare the small demo terminal and use `main.pdf` on the projector. The slides can be presented without a live backend. If demonstrating the integrated stack, rehearse it separately before class.

## Result comparisons

Slides 11–12 visualize the accepted **local RPKI representation experiment**. R1 stores full daily sets, R2 stores the initial day plus changes, and R3 adds a day-8 checkpoint to R2. All share the same transition sidecars, canonical encoding and compression. Charts use recorded measurements; generating them does not run the workload.

- Analytical storage: 47.76 / 3.56 / 6.95 MB for R1 / R2 / R3. R2 saves 92.6% against R1.
- All-days reconstruction: 6.73 / 6.88 / 6.76 seconds. Each value is the median of three build-level medians, each with three warm cycles. Whiskers show the range of build medians, not confidence intervals. Exact output digests match.
- Measured local retained files: 251.98 / 207.90 / 211.19 MB, including sources, coordinate maps, metadata and query scratch. R2 saves 17.5% on this broader accounting boundary. Deployed broker/checkpoint costs and filesystem overhead are outside this experiment figure.

These results support a storage trade-off within the selected RPKI scope. They do not establish a speedup, cold-cache latency, production disk savings, or a conclusion for all four data domains.

The charts are vector PDFs in `figs/`, with PNG previews. `plot-results.py` reads the accepted JSON evidence directly and writes the charts plus `figs/results-data.json` for numerical inspection. To regenerate using an environment with Matplotlib and NumPy:

```sh
python plot-results.py
sh build.sh
```

Sources: `workloads/results/phase20/rpki-representations.json`, `workloads/results/phase20/rpki-all-in-storage.json`; methodology: `docs/phase20-storage-query.md`.
