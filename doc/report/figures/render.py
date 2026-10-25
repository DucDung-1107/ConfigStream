#!/usr/bin/env python3
"""Render the editable SVG to a vector PDF and a 300-dpi PNG preview."""

import re
import subprocess
from pathlib import Path

here = Path(__file__).parent.resolve()
svg_path = here / "configstream-architecture.svg"
svg = svg_path.read_text(encoding="utf-8")
width, height = map(float, re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', svg).groups())
html_path = here / "_wrap.html"
html_path.write_text(
    f'<!doctype html><meta charset="utf-8"><style>@page{{size:{width}pt {height}pt;margin:0}}html,body{{margin:0;padding:0}}svg{{display:block}}</style>{svg}',
    encoding="utf-8",
)
chrome_candidates = [
    Path("C:/Program Files/Google/Chrome/Application/chrome.exe"),
    Path("C:/Program Files (x86)/Microsoft/Edge/msedge.exe"),
]
chrome = next(p for p in chrome_candidates if p.exists())
pdf_path = here / "configstream-architecture.pdf"
subprocess.run(
    [
        str(chrome),
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={pdf_path}",
        html_path.as_uri(),
    ],
    check=True,
    timeout=120,
)
subprocess.run(
    [
        "pdftoppm",
        "-png",
        "-r",
        "300",
        "-singlefile",
        str(pdf_path),
        str(here / "configstream-architecture"),
    ],
    check=True,
)
html_path.unlink()
print("rendered PDF and PNG")
