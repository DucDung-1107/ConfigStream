# Research dashboard information architecture

The persistent dashboard-01 inset sidebar links seven static App Router routes:
Overview (`/`), Infrastructure History (`/history/`), RPKI States (`/rpki/`),
RIS Events (`/ris/`), DNS Measurements (`/dns/`), Experiments (`/experiments/`),
Pipeline & Operations (`/operations/`). All routes share SiteHeader, SectionCards,
the reference Card/Chart/Table primitives, shared formatting and evidence detail
sheets. Neutral light styling and reference responsive sidebar behavior apply.

Overview cards retain four distinct native units; only RIS has the overview
source-minute chart. The scope table shows the semantic role and source qualifier.
Infrastructure uses current configuration metadata: exact asset/date filters,
server cursor pages and versions/diffs/alerts tabs. Repository/full private
incarnation search is not exposed by that API.

RPKI selects accepted complete dates and shows canonical membership/boundary
counts and reconstruction digests. RIS separates accepted minute aggregates from
four owned event identities/two fingerprints. DNS shows selected-object coverage,
second precision, worker restrictions, unknown entity coverage and descriptive
value differences. Experiments exposes exact registered storage/query/trial
receipts, negative findings and H3 NOT_RUN. Operations separates manual live API
readiness probes (60-second freshness) from recorded Kubernetes/recovery evidence.

No domain screen derives new state/event semantics in JavaScript. Detail sheets
trace approved values to fixed public receipt paths; copied IDs are safe metadata
or owned fixture identifiers. Aggregate tables filter their complete bounded
exports and page ten records. Large configuration histories use backend cursors.
