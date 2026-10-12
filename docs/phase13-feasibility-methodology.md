# Phase 13 — bounded dataset feasibility methodology

Round 1, checked 2026-10-12. This is source verification and data inspection,
**not integration**. The starting repository was clean `main` at
`e5cb3601c73d37feae384bd9eb6303e8e319a4d4`. Phase 10–12 data and conclusions
are unchanged. In particular, Phase 12's decision B remains valid for direct
configuration history.

## Research contract

The questions are availability, real temporal evidence, natural scale,
configuration relevance, reproducibility, architecture fit, complementarity
and academic defensibility. Every mandatory candidate appears exactly once in
[the registry](../workloads/results/dataset-feasibility/candidate-registry.json):
15 configuration/archive candidates, three operational-policy candidates,
two DNS candidates and three routing candidates: **23 artifacts/services**.
This is not 23 independent underlying populations.

T0 = static real content; T1 = historical metadata; T2 = recoverable VCS
content; T3 = repeated real observations; T4 = operational policy state;
T5 = operational events; TX = controlled experiment activity. Classifications
can overlap. T2 can describe a published archive that we did not download;
that is not a claim of locally recovered revisions.

F1 = direct file snapshot fit; F2 = adaptable state observation;
F3 = native event workload; F4 = high-cost archive/discovery integration.
Access to a landing page, metadata, raw artifacts and a live payload are
recorded separately. A documented service does not establish successful capture.

## Resources and safeguards

[Environment](../workloads/results/dataset-feasibility/environment.json):
Apple M3 Pro, arm64 macOS, 12 logical cores, 19,327,352,832 RAM bytes,
Python 3.12.14. Initial free disk was approximately 632 GiB; measured final
free disk before final documentation was 676,387,565,568 bytes. Raw probe cache
was 623,756,307 bytes. No Kafka, Spark, MinIO or Elasticsearch service was
started for these probes. Those runtimes are not performance-test variables
in Round 1.

Downloads are allowlisted HTTPS, at most 500,000,000 bytes per artifact,
45-second socket timeout and five-minute download deadline. A 20 GiB disk
reserve is checked before each download. Run commands serially: the cache
does not implement cross-process locking. Partial downloads are discarded;
completed artifacts are reusable and checked. The unique primary probe
artifacts total **618,946,129 bytes**; this excludes small metadata/PDF files
and retries. Exact network transfer is unknown: one duplicate Helm download
occurred. The roughly 2 GB target was not approached; the 39 GB Compose
archive, TerraDS redownload, full SWH graph and cloud queries were not attempted.

External files are data only. No Terraform, Helm, Ansible, Kubernetes,
Compose workload, research notebook or downloaded script was executed.
Raw data stays in ignored `.cache/phase13/`; committed outputs contain only
counts, schemas, hashes, dates, sizes and safe provenance. Contacts, DNS TXT
values, raw domain payloads and source configurations are not published.

## Exact probes and counting policies

### RPKI

Fourteen consecutive RIPE NCC trust-anchor observations, 2026-09-01 through
2026-09-14, from `ripencc.tal/YYYY/MM/DD/roas.csv.xz`. Published SHA-256
sidecars and local SHA-256 were checked. XZ integrity and UTF-8 CSV parsing
were checked. Schema: `URI, ASN, IP Prefix, Max Length, Not Before, Not After`.
Identity is `(trust-anchor scope, numeric ASN, canonical prefix, maxLength)`.
The URI and certificate validity are not authorization identity. This is one
anchor, **not global RPKI coverage**.

For consecutive sets S and T, additions are `T - S`, removals `S - T`, and
unchanged `S & T`. Daily change ratio is `|S symmetric_difference T| / |S union T|`.
The first day is a baseline, not changes. Missing dates are reported and not
bridged. Repeated rows across days are observations, not new authorizations.
Daily ASN concentration counts tuple additions/removals, not operator actions.
Validated results can change through certificates, validators or repositories.
See [RIPE's archive description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md).

### BGP

Twelve five-minute `rrc00` RIS update archives, 2026-09-01
00:00:00–01:00:00 UTC, using `mrtparse==2.2.0`. Gzip CRC and decompressed MRT
bytes were checked. We count individual peer-specific unicast NLRI
announcements/withdrawals, including MP_REACH/MP_UNREACH, **not UPDATE messages**.
Integer source epoch is used, not the parser's locally formatted date string.
The rate denominator is the fixed 3,600-second source window, not download,
parsing or replay duration.

Fingerprint: source-second, peer ASN/IP, action, prefix, optional path ID,
path attributes. Duplicate fingerprints are reported, not silently removed:
repeated announcements in one second can be genuine observations. Invalid
records are excluded and counted. There were 57 parser errors; all archive
CRCs passed. Counts are a parsed lower bound, not proof that the archive itself
is malformed. Out-of-order timestamps refer to observed message ordering.
See [RIS MRT documentation](https://www.ris.ripe.net/docs/mrt/).

### DNS

Two complete published `.li` open-zone Parquet partitions, 2026-09-01 and
2026-09-02. `.li` was selected before inspection because two modest complete
partitions fit the budget; it is not representative of all DNS. No arbitrary
subset of Parquet rows was substituted for a daily zone observation.

All rows and record types are counted. For state comparisons use successful
status-zero A/AAAA rows only, grouped by lowercase, trailing-dot-stripped
`(query_name, query_type, response_name, response_type)` and canonical IP set.
TTL, timestamps, workers, DNSSEC signatures and TXT values do not define
address-state identity. The comparison denominator is groups present on both
days. Newly observed and no-longer-observed groups are separate; they are not
asserted zone creations/deletions. Query names are not registered domains.

Report physical Parquet bytes, uncompressed column bytes, selected Arrow
buffers and canonical address JSON bytes separately. None is DNS wire traffic
or original configuration-file bytes. Worker times are source measurement
epoch milliseconds. Load balancing, cache, vantage and resolver effects
can produce differences without authoritative zone edits.
[Schema](https://openintel.nl/data/dictionary/),
[scope](https://openintel.nl/data/forward-dns/zone-based/).

### ZoneStream and blocked probes

Two explicit 30-second WebSocket sessions connected to `zone_diff`, without
credentials. Both received zero messages. This proves transport reachability,
not payload schema, historical replay or zero natural population velocity.
No unattended collector remains running. The first record's timestamp is its
finish time; the repeat script explicitly records its start time.

CAIDA PeeringDB historical access returned HTTP 401. We did not submit personal
information, obtain an agreement or bypass access; its requested 7–14-day
probe therefore has N/A records/changes. BigQuery table-list metadata returned
401; **no query or billable action** occurred. RouteViews archive listing was
reachable, but a second event corpus was unnecessary for this round. SWH API
index was reachable; no actual historical content was recovered.

### Research artifacts and existing histories

Pinned Git CSVs and Zenodo ZIP/CSV/TXT artifacts were inspected as data.
Zenodo published MD5, artifact size, local SHA-256 and ZIP CRC were verified.
CSV delimiter, quoted newlines and headerless logs are handled explicitly.
Metadata rows are not recovered historical contents. Nested Helm metadata
contains content URLs, not every version's chart archive.

Five first-listed IaC bug fixes were examined without replacing unhelpful
cases. No before/after deployment-configuration pair was recovered; three
selected YAML changelog creations have no parent blob, one fix had no relevant
extension and one source license was unknown. The failure is retained.
Phase 11/12 histories and Batfish summaries were reused with recorded file
hashes; no expensive extraction was repeated.

## Reproduction

Use the project's locked environment. Optional probe parsers are transient,
not additions to production dependencies. Outputs can be directed to ignored
paths to avoid replacing the original measurement record:

```sh
make feasibility-test
make feasibility-report
.venv/bin/python -m workloads.feasibility rpki --output .cache/phase13/rpki-repeat.json
.tools/uv/bin/uv run --no-sync --with mrtparse==2.2.0 python -m workloads.feasibility bgp --output .cache/phase13/bgp-repeat.json
.venv/bin/python -m workloads.feasibility_artifacts --output .cache/phase13/artifacts-repeat.json
# Read OpenINTEL's terms before giving explicit acceptance:
.tools/uv/bin/uv run --no-sync --with pyarrow==19.0.1 python -m workloads.feasibility_dns --download --accept-openintel-terms --output .cache/phase13/dns-repeat.json
.tools/uv/bin/uv run --no-sync --with websocket-client==1.9.0 python -m workloads.feasibility_dns --job zonestream --output .cache/phase13/zone-repeat.json
```

[Probe manifest](../workloads/results/dataset-feasibility/probe-manifest.json)
pins exact URLs, sizes and SHA-256; parser versions and runtime are in individual
results. Report generation is offline and deterministic from committed
measurements. A repeat download/parse is not expected to reproduce runtime.
No live request occurs in unit tests or normal CI.

## Evidence, scoring and uncertainty

Scores are judgments, not estimators: authenticity/temporal 20, natural scale
20, access/reproducibility 15, infrastructure relevance 15, fit 10,
complementarity 10, license/security 10. A sensitivity band of ±5 total points
is heuristic, **not a confidence interval**. Scores do not override legal or
access blockers. Complementarity matrix is a disclosed semantic heuristic;
it is not correlation computed from independent dataset samples. Publication
counts, local measurements and population estimates occupy different fields.

This round does not estimate global routing, DNS or RPKI volumes from one
collector/zone/anchor. Contiguous small probes cannot prove decade-wide
completeness or format stability. Inter-change timing is daily-censored for
snapshots; BGP has event times. Missing access/data is null/N/A, never zero.
Failure and correction history is in
[probe-audit.json](../workloads/results/dataset-feasibility/probe-audit.json).
