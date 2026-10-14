# Browser visual review

Primary reference screenshots: [desktop](screenshots/phase20v/reference-desktop.png),
[mobile](screenshots/phase20v/reference-mobile.png). Actual Chrome/Playwright
captured the official dashboard-01 preview; a networkidle timeout was corrected
to wait for documented content/chart/font readiness. Reference capture is not a
claim about its generic business data.

The ConfigStream shell was visually compared: same inset sidebar proportions,
header separation, neutral border/radius/card density, Geist typography and
chart/table relationship. All seven desktop screens plus Overview at1440×900,
768×1024 and390×844, detail/copy, empty filter and unavailable evidence states
are captured by `frontend/tests/review.mjs`.

Initial review found mobile chart header crowding; moving its range control below
the text on small widths resolved it. Tables use controlled horizontal scrolling
and mobile instructions. Browser tests exposed ARIA labels on unroled chart divs,
missing configuration tab panels and a nested main landmark. Those structures
were corrected rather than excluding accessibility rules.

On the deployed Kubernetes frontend, nineteen automated browser tests passed; seven page scans had
zero violations for selected WCAG2A/AA and2.1A/AA axe rules. Mobile navigation,
tablet/desktop overflow, keyboard Escape, copy, dates/ranges, filtering/sorting,
server cursor pagination, errors, DNS qualification and H3 were tested.
Automated checks do not certify all accessibility requirements. All14 screenshots
were visually inspected. Desktop/tablet/mobile have no page overflow; large tables
scroll inside bounded containers. The updated mobile chart control no longer
crowds its heading. Neutral cards, sidebar/header hierarchy, readable qualifiers,
and restrained chart/table density match the chosen block. No second aesthetic,
gradient decoration, fake business data or universal evidence unit was introduced.

The unmocked Kubernetes rehearsal verified two configuration versions and their
adjacent diff, fresh API READY, all seven routes, source-day/range controls,
processing/negative-result tabs, mobile navigation and safe-ID copy. Unexpected
console/page errors: zero. Only the deliberately routed aggregate503 appears in
the controlled error state. An initial favicon404 was fixed, not hidden by the
error filter. History screenshots now wait for returned API rows, and reference
screenshots wait for the complete SVG animation rather than an empty chart.

Receipts: [visual/screenshot checksums](../workloads/results/phase20v/visual-review.json),
[interactions/axe](../workloads/results/phase20v/interaction-tests.json),
[actual rehearsal](../workloads/results/phase20v/demo-rehearsal.json).
The19 deterministic browser tests route API/fault responses; they are distinct
from this unmocked configuration/API demonstration. Neither is a new external
source ingestion or multi-node recovery trial.
