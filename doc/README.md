# Contributed report materials

Report sources and supporting figures contributed by [Nguyễn Đức Dũng in PR #1](https://github.com/kongwoang/ConfigStream/pull/1).

- [Project report source](report/main.tex) and its [PDF](report/main.pdf).
- [Earlier checkpoint report](report/checkpoint_report.tex).
- [Project overview](project_overview.md), bibliography and supporting figures.

These are report drafts retained for further editing. The report PDF still needs layout cleanup, including the long-path table identified during review. The paper and older checkpoint text are not a new pipeline acceptance record; implementation status is maintained in [the architecture guide](../docs/architecture.md).

## Presentation to use

Use [Checkpoint 1 — 26 October 2026](../docs/presentations/checkpoint-2026-10-26/README.md) for the group presentation: 21 main slides, 2 appendix slides, and Vietnamese presenter notes. It remains the primary deck after integrating both contributions.

## Build the report

With a LaTeX distribution providing `latexmk`, from this directory:

```sh
cd report
latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=_build main.tex
```

Build logs, auxiliary files and QA captures are not source material and are ignored.
