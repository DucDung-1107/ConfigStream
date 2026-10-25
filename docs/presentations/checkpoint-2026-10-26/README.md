# Group 4 — first milestone checkpoint

13 English slides for 26 October 2026, with Vietnamese speaker notes. Planned speaking time: approximately 10 minutes.

- [Presentation PDF](main.pdf): use this when projecting.
- [Presenter PDF](speaker-notes.pdf): each page has the slide on the left and Vietnamese notes on the right.
- [Speaker notes](speaker-notes.md): convenient rehearsal copy.
- [Overview preview](preview.png): all 13 slides on one image.
- [LaTeX source](main.tex): edit slide text and `\vnote{...}` here.

The deck introduces the problem, demonstrates version ordering, introduces all four data types (Terraform, RPKI, RIS and DNS), explains their separate processing paths, and maps implemented progress to the three checkpoint requirements: executable code in Git, prepared data, and system architecture.

## Template and illustrations

The four SimplePlus theme files are copied unchanged from the supplied template, preserved in [../SimplePlus_Blank_Template](../SimplePlus_Blank_Template). The main architecture figure `four-paths` shows all four domain pipelines. The original `architecture` figure is retained as an editable detail of the configuration path. All architecture, version-order, workflow and replay diagrams have editable `.drawio` sources beside their exported PNGs in `figs/`.

`figs/dashboard.png` is a cropped capture of the actual local frontend on 25 October 2026. Its cards and chart display the accepted static research evidence bundle; it is not a live backend replay. Capture used Chromium, a 1440 × 1000 viewport, device scale factor 1.5, and clip `{x: 300, y: 70, width: 1110, height: 660}`. No application data was mocked for the screenshot.

## Build

Requires a LaTeX distribution with Beamer, Vietnamese T5 fonts and `latexmk` (tested with TeX Live 2026):

```sh
cd docs/presentations/checkpoint-2026-10-26
sh build.sh
```

On macOS with MacTeX, if the tools are not in `PATH`:

```sh
PATH=/Library/TeX/texbin:$PATH sh build.sh
```

The script builds both PDFs in `_build/` and copies the final files to this directory. The LaTeX sources, four theme files and `figs/` can also be uploaded together to Overleaf; choose `main.tex` and pdfLaTeX. Choose `speaker-notes.tex` for the presenter version. Rehearsal notes in Markdown and `preview.png` are convenience exports; update them when changing the deck.

## Evidence and scope

The deck distinguishes current executable code from previously recorded integrated runs:

| Claim | Repository source |
| --- | --- |
| Demonstration input and normalized diff | `processing/demo.py`, `processing/diff.py` |
| Severity of logging / telnet rules | `rules/default.yaml`, `tests/unit/test_rules.py` |
| Late-arrival handling with bounded retained state | `processing/state.py`, `spark/streaming/stateful.py` |
| Configuration storage / serving path | `spark/streaming/outputs.py`, `analytics/main.py`, `api/`, `frontend/` |
| 6,505 file histories; 10,188 accepted versions; 3,683 adjacent pairs | `workloads/results/terrads/history-summary.json` |
| Recorded local replay and 29,402,997 Parquet bytes | `docs/phase12-acceptance.md` |
| Accepted RPKI / RIS / DNS counts and scopes; dashboard evidence bundle | `frontend/public/research/v1/catalog.json` |

Rechecked on 25 October 2026:

```sh
make processing-demo
.venv/bin/python -m pytest tests/unit/test_state_logic.py tests/unit/test_diff.py tests/unit/test_rules.py -q --tb=short
# 178 passed
```

These commands verify the small Python processing path. The historical full Kafka/Spark replay was not rerun while preparing the slides. Git revisions are not deployment events. Recorded local / single-node evidence does not establish distributed scalability or high availability.

Before presenting, prepare the small demo terminal and use `main.pdf` on the projector. The slides can be presented without a live backend. If demonstrating the integrated stack, rehearse it separately before class.
