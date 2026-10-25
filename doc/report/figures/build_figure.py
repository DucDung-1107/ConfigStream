#!/usr/bin/env python3
"""Build the ConfigStream architecture figure.

The figure is deliberately organized as one causal pipeline:
typed input -> event-time reconstruction -> semantic diff -> publication,
with evaluation attached to the committed evidence.  It is an editable SVG
source intended for a two-column academic paper.
"""

from pathlib import Path
from xml.sax.saxutils import escape

W, H = 460, 320
INK = "#202b35"
MUTE = "#687582"
LINE = "#aab4bd"
PAPER = "#ffffff"
INPUT = "#e7f1f4"
INPUT_S = "#397783"
STATE = "#eaf0f7"
STATE_S = "#3f6f9b"
SEM = "#f8f0dd"
SEM_S = "#a97922"
AUTH = "#e8f2e8"
AUTH_S = "#4c7b50"
VIEW = "#f5e9ee"
VIEW_S = "#9a5268"
WARN = "#f2e5d9"
WARN_S = "#a66127"
SERIF = "'Times New Roman','Liberation Serif',Nimbus Roman,Times,serif"

out = []
add = out.append


def esc(s):
    return escape(str(s))


def text(x, y, s, size=6.4, anchor="start", fill=INK, weight=None, italic=False):
    # Permit the compact text(x, y, label, size, COLOR) form used below while
    # keeping the normal SVG anchor argument explicit when needed.
    if isinstance(anchor, str) and anchor.startswith("#"):
        fill, anchor = anchor, "start"
    attrs = []
    if weight:
        attrs.append(f'font-weight="{weight}"')
    if italic:
        attrs.append('font-style="italic"')
    extra = (" " + " ".join(attrs)) if attrs else ""
    add(
        f'<text x="{x:.2f}" y="{y:.2f}" font-family="{SERIF}" '
        f'font-size="{size:.2f}" fill="{fill}" text-anchor="{anchor}"{extra}>{esc(s)}</text>'
    )


def rect(x, y, w, h, fill=PAPER, stroke=INK, sw=0.65, rx=3, dash=None):
    extra = f' stroke-dasharray="{dash}"' if dash else ""
    add(
        f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" '
        f'rx="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="{sw:.2f}"{extra}/>'
    )


def line(x1, y1, x2, y2, stroke=INK, sw=0.75, dash=None, arrow=False):
    extra = []
    if dash:
        extra.append(f'stroke-dasharray="{dash}"')
    if arrow:
        extra.append('marker-end="url(#arrow)"')
    attrs = (" " + " ".join(extra)) if extra else ""
    add(
        f'<path d="M{x1:.2f},{y1:.2f} L{x2:.2f},{y2:.2f}" fill="none" '
        f'stroke="{stroke}" stroke-width="{sw:.2f}" stroke-linecap="round"{attrs}/>'
    )


def poly(points, stroke=INK, sw=0.75, dash=None, arrow=False):
    d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in points)
    extra = []
    if dash:
        extra.append(f'stroke-dasharray="{dash}"')
    if arrow:
        extra.append('marker-end="url(#arrow)"')
    attrs = (" " + " ".join(extra)) if extra else ""
    add(
        f'<path d="{d}" fill="none" stroke="{stroke}" stroke-width="{sw:.2f}" '
        f'stroke-linejoin="round" stroke-linecap="round"{attrs}/>'
    )


def circle(x, y, r, fill=PAPER, stroke=INK, sw=0.65):
    add(
        f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{r:.2f}" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="{sw:.2f}"/>'
    )


def label(x, y, letter, title):
    text(x, y, f"({letter})", 7.0, weight="bold")
    text(x + 18, y, title, 7.0, weight="bold")


def box(x, y, w, h, title, subtitle="", fill=PAPER, stroke=INK, title_size=6.3):
    rect(x, y, w, h, fill, stroke, 0.75, 3)
    text(x + w / 2, y + 10, title, title_size, "middle", weight="bold")
    if subtitle:
        text(x + w / 2, y + h - 6, subtitle, 5.55, "middle", MUTE)


def tag(x, y, w, title, fill, stroke):
    rect(x, y, w, 14, fill, stroke, 0.65, 2)
    text(x + w / 2, y + 9.5, title, 5.9, "middle", weight="bold")


add(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}pt" height="{H}pt">')
add("<title>ConfigStream architecture and evidence flow</title>")
add(
    "<desc>One left-to-right pipeline from typed snapshots through event-time "
    "state reconstruction and semantic diffing to an authoritative lake and "
    "rebuildable read projections, with evaluation attached to committed evidence.</desc>"
)
add(
    '<defs><marker id="arrow" viewBox="0 0 7 7" refX="6.2" refY="3.5" '
    'markerWidth="5" markerHeight="5" markerUnits="userSpaceOnUse" orient="auto">'
    '<path d="M0,0.5 L6.5,3.5 L0,6.5 z" fill="#202b35"/></marker></defs>'
)
add(f'<rect width="{W}" height="{H}" fill="{PAPER}"/>')

# Main causal pipeline. The vertical alignment is intentional: every arrow
# points to the next operation, while side outputs describe real failure and
# serving boundaries rather than decorative branches.
label(8, 18, "a", "typed snapshots")
label(111, 18, "b", "event-time state")
label(218, 18, "c", "semantic change")
label(326, 18, "d", "publication")

# (a) Input records
rect(6, 25, 95, 102, "#fbfcfc", LINE, 0.7, 4)
text(13, 36, "Kafka: config.snapshots", 6.2, weight="bold")
text(13, 46, "asset_id is the partition key", 5.7, MUTE)
tag(13, 55, 34, "asset", INPUT, INPUT_S)
tag(51, 55, 42, "snapshot", INPUT, INPUT_S)
rect(13, 76, 79, 38, PAPER, INPUT_S, 0.7, 3)
text(18, 86, "asset_id: router-07", 5.7)
text(18, 96, "version: 18    t_e: 10:04", 5.7)
text(18, 106, "payload + raw_hash", 5.7, MUTE)
line(101, 77, 111, 77, INK, 0.8, arrow=True)
text(52, 122, "typed input", 5.7, "middle", MUTE)

# (b) Event-time reconstruction
rect(111, 25, 98, 102, "#fbfcfe", LINE, 0.7, 4)
text(118, 36, "identity gate + version buffer", 6.0, weight="bold")
text(118, 46, "watermark; bind snapshot_id", 5.2, MUTE)
text(118, 52, "arrival: v18, v20, v19", 5.2, MUTE)
for i, (v, fill, stroke) in enumerate(
    (("v18", INPUT, INPUT_S), ("v20", WARN, WARN_S), ("v19", STATE, STATE_S))
):
    x = 119 + i * 27
    rect(x, 59, 22, 16, fill, stroke, 0.7, 2)
    text(x + 11, 69.8, v, 5.8, "middle", weight="bold")
    if i < 2:
        line(x + 22, 67, x + 26, 67, LINE, 0.55, arrow=True)
text(118, 82, "sort by producer version", 5.4, MUTE)
rect(119, 87, 73, 20, STATE, STATE_S, 0.8, 3)
for i, v in enumerate(("v18", "v19", "v20")):
    x = 123 + i * 23
    rect(x, 91, 19, 12, PAPER, STATE_S, 0.55, 2)
    text(x + 9.5, 99.2, v, 5.3, "middle", weight="bold")
    if i < 2:
        line(x + 19, 97, x + 22, 97, STATE_S, 0.55, arrow=True)
line(209, 77, 218, 77, INK, 0.8, arrow=True)
text(160, 116, "edges: v18 -> v19 and v19 -> v20", 5.35, "middle", MUTE)
text(160, 123, "latest accepted version = v20", 5.35, "middle", weight="bold")

# Diagnostics side path
poly([(160, 127), (160, 140), (144, 140)], WARN_S, 0.7, dash="2 1", arrow=True)
box(93, 133, 72, 25, "diagnostic", "gap / duplicate / eviction", WARN, WARN_S, 5.9)

# (c) semantic diff
rect(218, 25, 102, 102, "#fffdf9", LINE, 0.7, 4)
text(225, 36, "adjacent edges", 6.2, weight="bold")
tag(225, 47, 35, "v18", INPUT, INPUT_S)
tag(270, 47, 35, "v19", STATE, STATE_S)
line(260, 54, 270, 54, INK, 0.75, arrow=True)
tag(225, 59, 35, "v19", STATE, STATE_S)
tag(270, 59, 35, "v20", STATE, STATE_S)
line(260, 66, 270, 66, INK, 0.75, arrow=True)
rect(225, 77, 80, 16, SEM, SEM_S, 0.75, 3)
text(265, 87.5, "normalize + hash", 5.6, "middle", weight="bold")
line(265, 93, 265, 98, INK, 0.75, arrow=True)
rect(225, 99, 80, 17, PAPER, SEM_S, 0.75, 3)
text(265, 110, "diff + YAML rules", 5.6, "middle", weight="bold")
text(265, 124, "raw hash != normalized hash", 5.0, "middle", MUTE)
line(320, 77, 326, 77, INK, 0.8, arrow=True)

# (d) commit and serving
rect(326, 25, 128, 102, "#fbfcfa", LINE, 0.7, 4)
text(333, 36, "commit manifest", 6.2, weight="bold")
rect(333, 45, 114, 22, PAPER, AUTH_S, 0.8, 3)
text(390, 54, "query_id + batch_id", 5.7, "middle", weight="bold")
text(390, 62, "counts + file inventory", 5.4, "middle", MUTE)
box(333, 76, 52, 28, "Parquet", "authoritative", AUTH, AUTH_S, 6.0)
box(395, 76, 52, 28, "index/API", "rebuildable", VIEW, VIEW_S, 6.0)
line(390, 67, 359, 76, AUTH_S, 0.75, arrow=True)
line(390, 67, 421, 76, VIEW_S, 0.75, dash="2 1", arrow=True)
text(390, 118, "projection failure does not redefine history", 5.3, "middle", MUTE)

# Evaluation band attached to the actual committed evidence.
label(8, 181, "e", "evaluation from committed evidence")
rect(6, 188, 448, 85, "#fcfcfd", LINE, 0.75, 4)
text(14, 199, "same run manifest", 6.0, weight="bold")
line(82, 196, 96, 196, INK, 0.7, arrow=True)
box(98, 205, 70, 30, "oracle", "row equality", INPUT, INPUT_S, 6.0)
box(181, 205, 70, 30, "invariants", "identity / adjacency", STATE, STATE_S, 6.0)
box(264, 205, 70, 30, "resources", "bytes / time / RAM", SEM, SEM_S, 6.0)
box(347, 205, 94, 30, "diagnostics", "lag / gaps / recovery", WARN, WARN_S, 6.0)
line(168, 220, 181, 220, INK, 0.7, arrow=True)
line(251, 220, 264, 220, INK, 0.7, arrow=True)
line(334, 220, 347, 220, INK, 0.7, arrow=True)
text(
    224, 252, "report rates with their workload unit and run boundary", 6.0, "middle", weight="bold"
)
text(
    224,
    263,
    "files / rows / occurrences are not pooled into one capacity number",
    5.55,
    "middle",
    MUTE,
)

# Feedback path: evaluation constrains claims; it is not a model-training loop.
poly([(451, 235), (451, 294), (218, 294), (218, 287)], MUTE, 0.65, dash="2 1", arrow=True)
text(334, 290, "evidence constrains the claim", 5.55, "middle", MUTE, italic=True)

# Bottom legend, kept small and meaningful.
rect(8, 305, 9, 7, INPUT, INPUT_S, 0.55, 1.5)
text(21, 311, "input", 5.6)
rect(55, 305, 9, 7, STATE, STATE_S, 0.55, 1.5)
text(68, 311, "state", 5.6)
rect(102, 305, 9, 7, SEM, SEM_S, 0.55, 1.5)
text(115, 311, "semantic transform", 5.6)
rect(196, 305, 9, 7, AUTH, AUTH_S, 0.55, 1.5)
text(209, 311, "authoritative", 5.6)
rect(278, 305, 9, 7, VIEW, VIEW_S, 0.55, 1.5)
text(291, 311, "rebuildable view", 5.6)
line(380, 308.5, 400, 308.5, MUTE, 0.65, dash="2 1", arrow=True)
text(405, 311, "diagnostic / constraint", 5.6)

add("</svg>")
Path(__file__).with_name("configstream-architecture.svg").write_text(
    "\n".join(out), encoding="utf-8"
)
print("wrote configstream-architecture.svg")
