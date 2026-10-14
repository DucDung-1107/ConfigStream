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

Before deployment, eighteen automated browser tests passed; seven page scans had
zero violations for selected WCAG2A/AA and2.1A/AA axe rules. Mobile navigation,
tablet/desktop overflow, keyboard Escape, copy, dates/ranges, filtering/sorting,
server cursor pagination, errors, DNS qualification and H3 were tested.
Automated checks do not certify all accessibility requirements. Final Kubernetes
screenshots, rehearsal and review receipts must be recorded before full acceptance.
