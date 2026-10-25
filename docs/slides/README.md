# Experimental slide template and evidence tools

Contributed with [PR #1](https://github.com/kongwoang/ConfigStream/pull/1). This directory retains an experimental Beamer template and number-extraction tools. `deck.tex` contains generic demonstration content; it is not the group's checkpoint presentation.

**Present [the maintained checkpoint deck](../presentations/checkpoint-2026-10-26/README.md).** That directory contains the current English slides, Vietnamese notes, architecture diagrams and comparison charts.

## Useful contents

- `tools/extract_numbers.py`: checks recorded experiment values against the contributed paper and exports CSV/LaTeX data.
- `data/VERIFICATION.md`: the associated number-verification record.
- `theme/`, `charts/`, `figs/`: reusable presentation examples.
- `shoot.py`, `check_deck.py`: render and inspect the experimental template.

The template can be built with `sh build.sh` using LuaLaTeX. Generated template PDFs are not official project slides. The earlier planning draft is [../slide_plan.md](../slide_plan.md); it does not replace the current checkpoint source.
