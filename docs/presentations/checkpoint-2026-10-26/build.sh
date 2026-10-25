#!/bin/sh
set -eu
cd "$(dirname "$0")"
mkdir -p _build
latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=_build main.tex
latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=_build speaker-notes.tex
cp _build/main.pdf main.pdf
cp _build/speaker-notes.pdf speaker-notes.pdf
