#!/usr/bin/env python3
"""Build the ConfigStream platform architecture figure (16:9, editable SVG).

Layout is a strict grid: four evidence lanes (rows) pass through five stages
(columns): source evidence -> claim gate -> typed record -> authoritative
commit -> rebuildable consumers.  Every connector is a straight horizontal
arrow between two card edges; nothing is routed, dashed or diagonal.
"""
from pathlib import Path
from xml.sax.saxutils import escape

W, H = 1600, 900
INK, MUTE = "#17212b", "#536575"
C = {  # fill, stroke
    "input": ("#e8f2f5", "#3b7180"),
    "gate": ("#f7f0df", "#a97922"),
    "record": ("#eaf1f8", "#3f6f9b"),
    "commit": ("#e9f3e8", "#4c7b50"),
    "view": ("#f4e9ee", "#9a5268"),
}
COLS = {"input": (40, 260), "gate": (350, 360), "record": (760, 260),
        "commit": (1070, 210), "view": (1340, 220)}
ROW_Y, ROW_H, GAP = 170, 110, 22
rows = [ROW_Y + i * (ROW_H + GAP) for i in range(4)]

out = []
add = out.append


def t(x, y, s, size=16, fill=INK, weight=None, anchor="start", spacing=None):
    extra = ""
    if weight:
        extra += f' font-weight="{weight}"'
    if spacing:
        extra += f' letter-spacing="{spacing}"'
    add(f'<text x="{x}" y="{y}" font-size="{size}" fill="{fill}" text-anchor="{anchor}"{extra}>{escape(s)}</text>')


def card(kind, x, y, w, h):
    f, s = C[kind]
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{f}" stroke="{s}" stroke-width="2.4"/>')


def arrow(x1, x2, y, stroke=INK):
    add(f'<path d="M{x1} {y}H{x2}" stroke="{stroke}" stroke-width="2.6" fill="none" marker-end="url(#a)"/>')


def box(x, y, w, h, label, stroke):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="#fff" stroke="{stroke}" stroke-width="2"/>')
    t(x + w / 2, y + h / 2 + 5, label, 15, weight=700, anchor="middle")


add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="Arial,Helvetica,sans-serif">')
add("<title>ConfigStream evidence gates and authoritative publication</title>")
add("<desc>Four evidence lanes (configuration, RPKI, RIS, DNS) each pass a claim gate and become a typed temporal record. "
    "All lanes are committed once to authoritative Parquet datasets and a manifest. Analytics and search/API/dashboard read the committed history and can be rebuilt.</desc>")
add('<defs><marker id="a" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" markerUnits="userSpaceOnUse" orient="auto">'
    '<path d="M0 0L10 5L0 10z" fill="#17212b"/></marker></defs>')
add(f'<rect width="{W}" height="{H}" fill="#fff"/>')

t(40, 56, "ConfigStream: claim gates turn evidence into auditable temporal records", 32, weight=700)
t(40, 88, "Each lane keeps its own identity and temporal condition. One commit is authoritative; search and API are rebuilt from it.", 17, MUTE)

heads = [("input", "01  SOURCE EVIDENCE"), ("gate", "02  CLAIM GATE"), ("record", "03  TYPED RECORD"),
         ("commit", "04  COMMIT"), ("view", "05  CONSUMERS")]
for k, s in heads:
    x, w = COLS[k]
    t(x, 140, s, 15, MUTE, 700, spacing=1.2)
    add(f'<path d="M{x} 150H{x + w}" stroke="{C[k][1]}" stroke-width="3"/>')

lanes = [
    ("ConfigSnapshot", "asset · version · payload", "Version adjacency", "buffered producer order",
     "ConfigTransition", "identity · v → v+1"),
    ("RPKI source rows", "coordinate rows", "RPKI completeness", "coordinate inventory",
     "RPKITransition", "identity · complete state"),
    ("MRT frame", "peer · prefix · occurrence", "RIS multiplicity", "peer-prefix occurrences",
     "RISObservation", "peer · prefix · occurrence"),
    ("DNS measurement", "address · vantage · time", "DNS qualification", "group-level evidence",
     "DNSMeasurementGroup", "identity · uncertainty"),
]
gx, gw = COLS["gate"]
for i, (st, ss, gt, gs, rt, rs) in enumerate(lanes):
    y = rows[i]
    for k, title, sub in (("input", st, ss), ("gate", gt, gs), ("record", rt, rs)):
        x, w = COLS[k]
        card(k, x, y, w, ROW_H)
        t(x + 20, y + 36, title, 19, weight=700)
        t(x + 20, y + 62, sub, 15, MUTE)
    t(COLS["record"][0] + 20, y + 90, "+ provenance attached", 14, MUTE)
    # lane connectors: straight, card edge to card edge
    cy = y + ROW_H / 2
    arrow(COLS["input"][0] + COLS["input"][1] + 2, gx - 4, cy)
    arrow(gx + gw + 2, COLS["record"][0] - 4, cy)
    arrow(COLS["record"][0] + COLS["record"][1] + 2, COLS["commit"][0] - 4, cy)

# gate visuals, drawn inside the gate cards (bottom half, left of caption)
s = C["gate"][1]
y = rows[0] + 72
for j, v in enumerate(("v18", "v19", "v20")):
    box(gx + 20 + j * 74, y, 44, 26, v, s)
    if j < 2:
        add(f'<path d="M{gx + 66 + j * 74} {y + 13}H{gx + 90 + j * 74}" stroke="{s}" stroke-width="2" marker-end="url(#a)"/>')
t(gx + 238, y + 11, "emit only", 14, MUTE)
t(gx + 238, y + 28, "18→19, 19→20", 14, MUTE)

y = rows[1] + 94
add(f'<path d="M{gx + 20} {y}H{gx + 212}" stroke="{s}" stroke-width="3"/>')
for j in range(5):
    add(f'<path d="M{gx + 20 + j * 48} {y - 6}V{y + 6}" stroke="{s}" stroke-width="2.4"/>')
t(gx + 116, y - 14, "1 … N", 16, s, 700, "middle")
t(gx + 238, y - 10, "complete before", 14, MUTE)
t(gx + 238, y + 7, "transition", 14, MUTE)

y = rows[2] + 85
for j in range(3):
    add(f'<circle cx="{gx + 34 + j * 52}" cy="{y}" r="13" fill="#fff" stroke="{s}" stroke-width="2.4"/>')
    if j < 2:
        add(f'<path d="M{gx + 47 + j * 52} {y}H{gx + 73 + j * 52}" stroke="{s}" stroke-width="2.4"/>')
t(gx + 238, y - 2, "keep every", 14, MUTE)
t(gx + 238, y + 15, "occurrence", 14, MUTE)

y = rows[3] + 72
box(gx + 20, y, 100, 26, "coverage = ?", s)
box(gx + 130, y, 100, 26, "vantage = ?", s)
t(gx + 244, y + 11, "uncertainty", 14, MUTE)
t(gx + 244, y + 28, "kept", 14, MUTE)

# commit card spans all four lanes
cx, cw = COLS["commit"]
ch = rows[3] + ROW_H - rows[0]
card("commit", cx, rows[0], cw, ch)
mid = rows[0] + ch / 2
cs = C["commit"][1]
for j in range(3):
    add(f'<rect x="{cx + 70 + j * 12}" y="{mid - 120 - j * 12 + 24}" width="70" height="44" rx="4" fill="#fff" stroke="{cs}" stroke-width="2"/>')
t(cx + cw / 2, mid - 20, "Parquet datasets", 20, weight=700, anchor="middle")
t(cx + cw / 2, mid + 8, "+ manifest", 20, weight=700, anchor="middle")
t(cx + cw / 2, mid + 52, "SHA-256", 15, MUTE, anchor="middle")
t(cx + cw / 2, mid + 74, "row / file counts", 15, MUTE, anchor="middle")
t(cx + cw / 2, mid + 96, "source provenance", 15, MUTE, anchor="middle")
t(cx + cw / 2, mid + 140, "single recovery boundary", 15, cs, 700, "middle")

# consumers read the commit
vx, vw = COLS["view"]
for y, title, sub in ((rows[1], "analytics /", "reconciliation"), (rows[3], "search / API /", "dashboard")):
    card("view", vx, y, vw, ROW_H)
    t(vx + 20, y + 40, title, 19, weight=700)
    t(vx + 20, y + 64, sub, 19, weight=700)
    t(vx + 20, y + 92, "read-only · rebuildable", 14, MUTE)
    arrow(cx + cw + 2, vx - 4, y + ROW_H / 2, C["view"][1])
    t(cx + cw + 30, y + ROW_H / 2 - 10, "reads", 13, C["view"][1], anchor="middle")

# legend
ly = 740
for j, (k, label) in enumerate((("input", "source evidence"), ("gate", "claim gate"), ("record", "typed record"),
                                 ("commit", "authoritative"), ("view", "rebuildable"))):
    x = 40 + j * 230
    add(f'<rect x="{x}" y="{ly}" width="26" height="18" rx="4" fill="{C[k][0]}" stroke="{C[k][1]}" stroke-width="2"/>')
    t(x + 36, ly + 14, label, 15, MUTE)

add(f'<rect x="40" y="800" width="1520" height="56" rx="12" fill="#fff" stroke="#c4ced6" stroke-width="2"/>')
t(60, 835, "Not drawn: RPKI, RIS and DNS never pass through the configuration version buffer.", 18, INK, 700)
add("</svg>")

Path(__file__).with_name("configstream-platform-architecture.svg").write_text("\n".join(out), encoding="utf-8")
print("wrote configstream-platform-architecture.svg")
