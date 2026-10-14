# Dashboard data contracts and publication

`dashboard/export.py` reads seventeen explicitly pinned public receipts whose
SHA-256 values were verified against accepted commit
`9c2520c06aa3b455577ead9ef16f0a820e1e804d`. `source-bindings.json` freezes those
hashes. Missing/modified inputs fail export. Pydantic validates completeness,
native identities, DNS uncertainty and H3. The exported snapshot contains bounded
aggregate fields, source paths/hashes/code versions, qualifications and owned
fixture projections; no private source cache or full row ledger is read.

The browser validates a strict bounded Zod schema and the exact snapshot SHA-256
compiled into its application. Corrupt/missing bundles show unavailable, with no
cached success fallback. Snapshot build time, historical source windows, recorded
execution date and live probe timestamp have different labels.

Research source URLs resolve only fixed approved GitHub receipt paths at the
accepted commit. The static snapshot is an approved presentation export, not a
second historical source of truth. Updating it requires export, validation and a
new image. A completed selected DNS object still has unknown entity coverage;
zero matched-vantage eligibility and no real proven-empty evidence are explicit.

Configuration metadata uses existing read-only `/api/v1/snapshots`, `/diffs`,
`/alerts`, and `/assets/.../latest-snapshot` contracts. Default asset is the owned
`k8s-owned-router`. Filters are exact asset ID/UTC dates, request limit ten, cursor
pagination; Zod rejects raw-content fields or oversized output. Existing API
limits remain enforced. The browser receives no Kafka/S3/kubeconfig credentials.

Same-origin Nginx proxies permitted GET routes and API health only. Other API
paths/write methods are denied. Kubernetes metrics and lag are recorded evidence,
not a privileged monitoring backend. Fresh readiness expires after sixty seconds;
failed probes discard earlier success. There is no rapid automatic polling.

## Owned RIS regeneration finding

Python's gzip wrapper OS byte differed on macOS (19) versus the accepted Linux
fixture (3), changing compressed artifact identity despite equal MRT bytes.
Export initially failed its cross-check. The owned fixture generator now declares
the accepted Linux wrapper byte and requires exact original source projection
digest `2c783ae377357486b7c29de61bde2716bc65efee648856ed4a638926cdb8dfde`.
Its artifact SHA is `ab426552de7f614dc00b9ff76ef3fb2615a961f76dd2a1a2adcaadcb3d57055c`.
This recreates the pinned owned bytes; no RIS identity/parser/counting policy was
changed. Different compressed source artifacts would remain different identities.

Real DNS/RIS rows, query-name ledgers, config bodies, source archives and credentials
are absent from exports. Attribution is accessible on Overview; OpenINTEL aggregate
use retains prior CC BY-NC-SA4.0 qualifications. Instructor approval is unresolved.
