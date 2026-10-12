# Phase 13 — comprehensive real-world dataset survey

Round 1 verification date: **2026-10-12**. All 23 mandatory candidates were
examined; raw access, recovered contents and publisher descriptions are not
interchangeable. Definitions, guards and exact parsing policies are in
[methodology](phase13-feasibility-methodology.md). The full machine-readable
[registry](../workloads/results/dataset-feasibility/candidate-registry.json),
[primary-source verification](../workloads/results/dataset-feasibility/source-verification.json)
and [measured summary](../workloads/results/dataset-feasibility/summary.md)
are the reproducible evidence record.

## Complete inventory

“Raw yes” means a verified artifact or previously measured source is available,
not necessarily locally downloaded this round. “Metadata” means only metadata
or service protocol was verified. Historical dates below are published/listed
coverage, not a completeness guarantee. E = ecosystem/geographic scope.

| ID / primary source | Real data / historical content | Natural events? | Raw access | Date / E | Scale and license | Fit / decision |
|---|---|---|---|---|---|---|
| [A1 TerraDS v2](https://zenodo.org/records/20339474) | T0 current Terraform | No temporal claim | Yes, reused | 2026 v2; public IaC | 82,749 repos, 1,513,378 `.tf`; CC-BY-4 artifact, separate source licenses | F1 / baseline |
| [A2 TerraDS Git sample](phase12-acceptance.md) | T2 recovered Git content | No deployment claim | Yes, reused | Pinned repository histories | 500 sampled / 375 accepted repos; 10,188 revisions; filtered source licenses | F1 / primary |
| [A3 FRR/NGINX](real-history-sources.md) | T2 recovered Git content | No deployment claim | Yes, reused | Curated network/application test scope | 1,906 incarnations / 2,525 revisions; scope-specific licenses | F1 / baseline |
| [A4 Batfish](https://github.com/batfish/batfish) | T0 public parser/lab configurations | No | Yes, reused | Pinned curated network syntax | 324 usable files; Apache-2.0 | F1 / baseline |
| [A5 IaC Bug Study](https://github.com/gdrosos/iac-bug-study-artifact) | T1 bug/fix references, upstream recovery possible | Bug-selected, not natural frequency sample | Yes CSV, no recovered pair | 2024 pinned artifact; Ansible/Puppet/Chef | 360 bug rows; MIT artifact, source licenses separate | F1 / secondary |
| [A6 Compose evolution](https://github.com/SAILResearch/replication-21-ibrahim-dockercompose) | T1 commit/feature CSV | No direct operational events | Yes CSV | 2021 pinned study cohort | 16,283 cohort / 5,139 selected rows; no root license identified | F1 / secondary |
| [A7 Compose EMSE](https://zenodo.org/records/10648448) | T2 published bare Git mirror | VCS, not production stream | Yes, full mirror not downloaded | 2024-02-12 archive | 218 folders described; 39,003,853,032-byte mirror; CC-BY-4 artifact | F1 / secondary |
| [A8 ETH Compose population](https://zenodo.org/records/7194189) | T1 repository/version/experiment metadata | No natural changes established | Yes metadata | 2022-10-13 public GitHub population | 433,769 repository lines; CC-BY-4 | F1 / low priority |
| [A9 Kubernetes paper](https://vtechworks.lib.vt.edu/bitstreams/3567356d-63d6-4165-8f1d-f4ce6c5104a1/download) | T0/T1 security manifest study, not evolution corpus | No | Small artifact linked | 2023; public manifests | Paper 2,039 manifests / 92 repos; linked artifact MIT | F1 / low priority |
| [A10 Helm](https://zenodo.org/records/7552697) | T1 releases and content URLs | Release metadata, not deployment events | Yes metadata ZIP | 2023-01-19 Artifact Hub charts | 9,485 chart rows / 166,772 version rows; CC-BY vs CC-BY-SA conflict | F1 / secondary |
| [A11 Kubernetes audit](https://github.com/hkerma/kubernetes-event-dataset) | TX real system logs from scripted actions | **Not natural production activity** | Yes CSV | 2024 pinned PerfSPEC artifact | 16,564 audit rows inspected; CC-BY-4 | F3 / rejected as natural evidence |
| [A12 Apache/MySQL](https://github.com/tianyin/configuration_datasets) | T0 user-posted examples | No history | Tree/files available, content not probed | 2014 collection; forums/lists | 168 `.conf` + 260 `.cnf` packaged; no root license | F1 / low priority |
| [A13 BPlan](https://zenodo.org/records/22747162) | T0/T1/TX methodology and adapted examples | Not independent natural population | Yes ZIP | 2026-09-16; TerraDS-derived | 13,545,725 bytes; Apache-2.0 artifact | F1 / academic reference |
| [A14 Software Heritage](https://docs.softwareheritage.org/) | T2 archive graph/content documented | VCS only | API index, not recovered content | Global software; graph versions separately dated | No configuration-specific total measured; API/source terms | F4 / academic reference |
| [A15 BigQuery GitHub](https://codelabs.developers.google.com/codelabs/bigquery-github) | T0/T1 documented files/content/commits | Not historical blob guarantee | Metadata docs; anonymous table list 401 | Public GitHub; current inventory unresolved | Current size not verified; billing/source-license constraints | F4 / low priority |
| [B1 RPKI](https://ftp.ripe.net/rpki/) | T4 daily validated authorization state | State differences, not commands | Yes, parsed | Listed RIPE-anchor years 2011–2026 | 14-day probe; RIPE attribution/educational-use terms | F2 / primary |
| [B2 PeeringDB](https://www.caida.org/catalog/datasets/peeringdb/) | T3/T4 daily infrastructure records | Metadata/policy edits, not physical events | Registration/agreement, 401 here | Since 2010-07-29; global peering | SQL/SQLite/JSON transitions; CAIDA AUA, PII | F2 / access blocked |
| [B3 Two Campuses](https://pages.cs.wisc.edu/~akella/papers/uw-gatech-config.pdf) | T4 operational configuration archive described | Real campus configuration evolution | **Raw unavailable** | Five-year study; Georgia Tech / UW Madison | Paper 1,097 / 1,624 devices; no raw redistribution grant | F1 / academic reference |
| [C1 OpenINTEL](https://openintel.nl/) | T3 repeated observed DNS | Observation differences, uncertain cause | Yes open scopes; closed scopes separate | Project since 2015; `.li` since 2020-05-19 | Two complete `.li` days parsed; CC-BY-NC-SA-4 terms | F2 / primary |
| [C2 ZoneStream](https://openintel.nl/data/zonestream/) | T5 documented zone-change stream | Documented yes, no payload received here | Public handshake; payload/history unverified | Selected ccTLDs; live service | Two 30-second zero-message sessions; OpenINTEL terms | F3 / secondary |
| [D1 RIPE RIS](https://data.ris.ripe.net/) | T3 RIB / T5 MRT updates | **Yes routing observations** | Yes, parsed | `rrc00` listings from 1999.09; multiple collectors | 4,725,156 prefix events in one hour; RIPE terms | F3 / primary |
| [D2 RouteViews](https://archive.routeviews.org/) | T3 RIB / T5 MRT updates | Yes routing observations | Archive yes; metadata-only probe | Collector-specific long archive | No local event count; source terms need confirmation for redistribution | F3 / secondary |
| [D3 BGPStream](https://bgpstream.caida.org/data) | T3/T5 access framework over providers | Same underlying RIS/RouteViews events | Framework/docs verified | Provider-dependent | Not an independent corpus; tool/data terms separate | F3 / low priority |

### Identity corrections and configuration families

**TerraDS:** primary v2 record published 2026-05-26, DOI
[10.5281/zenodo.20339474](https://doi.org/10.5281/zenodo.20339474).
Reported 394,004 modules and 2,497,629 resource declarations accompany the
82,749-repository population. Phase 12 actually measured 1,513,378 `.tf`
files, 2,601,501,820 source bytes and 868,124 unique contents. Its separate
observed sample has 6,505 incarnations, 10,188 revisions, 3,683 adjacent pairs
and 12,278,085 bytes. Static resources are not revisions. Sampling estimates
are not locally collected population histories. See
[Phase 12 acceptance](phase12-acceptance.md) for immutable source checksums,
the rejection funnel, uncertainty and conclusion B.

**IaC bugs:** the MIT artifact at
`994c6efd581f1802b578a61feb5c774d93aed44d` contains repository lists and 360
bug rows. Parsed list rows: Ansible roles 33,355, Ansible 1,880, Chef 2,818,
Puppet 7,471. Strict HTTP(S) GitHub origin counts within those lists are
32,846, 1,556, 2,735 and 6,319; they are **not disjoint populations**.
URL protocols/API paths affect canonicalization; do not sum these as unique
repos. Bug rows reference 201 origins under that parser. Five bounded fixes
were inspected; zero before/after configuration pairs recovered. Tool Python
fixes and YAML changelog additions illustrate why extensions/PR counts are
not configuration changes. [Source and study artifact](https://github.com/gdrosos/iac-bug-study-artifact).

**Compose evolution:** pinned SAIL artifact
`589c1f44f277e9400ec8083a2ae8250dd6176142` yields 16,283 root-cohort rows,
5,139 `commit_count_100` selection rows and 70,002 feature-change rows.
Schema includes commit hash, changed feature and status. A feature row is not
necessarily a distinct commit, file version or recovered body. No original
frequency statistic was independently reproduced from file contents. Root
license evidence is missing, and upstream content recovery is untested.
[Replication source](https://github.com/SAILResearch/replication-21-ibrahim-dockercompose).

**Compose EMSE:** the archived `repos.7z` is exactly 39,003,853,032 bytes;
published MD5 `8d2ece68b5c0680901a9bb9e1725be57`. README describes 218 bare
repository folders. The 90,734-byte extraction ZIP was downloaded and checked,
but none of its scripts executed. A small ZIP of scripts is not the archived
Git content. Bounded extraction from the large 7z archive is not demonstrated.
Artifact CC-BY-4 does not replace each repository's source license.
[Zenodo 10648448](https://zenodo.org/records/10648448).

**ETH population:** two downloaded artifacts have 433,769 unique repository
lines and 767,622 semicolon-delimited execution rows. Schema:
`provider;repository;version;compose-file;success;n_packets`. These are
repository/experiment metadata, **not 433,769 recovered Compose files** or
767,622 historical changes. ZIP/CSV publication claims cannot manufacture
missing file contents. Adds less than TerraDS for our present purpose.
[Zenodo 7194189](https://zenodo.org/records/7194189).

**Kubernetes identity correction:** the supplied Virginia Tech PDF is
*Security Misconfigurations in Open Source Kubernetes Manifests*,
[DOI 10.1145/3579639](https://doi.org/10.1145/3579639), not a longitudinal
manifest-evolution dataset. The paper reports 2,039 manifests in 92 repos.
Its [SLIKUBE replication metadata](https://api.figshare.com/v2/articles/17425478)
links v3, 2023-01-13, MIT, a 344,180-byte ZIP. Landing metadata was verified;
historical content was not recovered. We do not invent another evolution
cohort to fill the mislabeled candidate.

**Helm:** downloaded 124,957,819-byte ZIP (published MD5 and CRC verified).
Actual gzip-CSV inspection found 9,485 chart records and 166,772 release-version
records. `helm_chart_content.csv` has **content URLs**, not every release's
body. Nested JSON is package metadata. Image-tag and vulnerability rows are
not chart revisions. Zenodo's CC-BY-4 metadata conflicts with description's
CC-BY-SA data/GPL code distinction; resolve before redistribution. External
release URL survival and completeness need a separate bounded pilot.
[Zenodo 7552697](https://zenodo.org/records/7552697).

**Audit logs:** PerfSPEC artifact
`e39cc98e8b27b6862a3f32a85a17565265db36d5` is the located Kubernetes
audit/event source. Actual inspection: 16,564 headerless audit CSV rows and
21,642 nonempty event lines. Controlled Helm deployment actions generated
real system logs; provenance does not establish natural production traffic.
Operation logs alone do not guarantee a complete resource-state history.
Reject only its use as **natural** workload evidence, not its validity as TX.
[Official artifact](https://github.com/hkerma/kubernetes-event-dataset).

**Apache/MySQL:** pinned tree has 168 httpd `.conf` and 260 MySQL `.cnf`
packaged files. README's 311/823 totals also include externally linked
examples; neither figure is historical revision count. Public forum/mailing
list provenance entails credentials, paths and privacy risk. No root license
identified. No bodies redistributed or before/after corpus established.
[Configuration datasets](https://github.com/tianyin/configuration_datasets).

**BPlan:** independently verified 2026-09-16 publication, 13,545,725-byte ZIP,
185 members. It supplies TerraDS exploration/change-mining methodology and
five adapted module examples, including manually altered actions for plan
experiments. We read code/README as data and ran no Terraform. This is an
auxiliary methodology artifact with shared ancestry, **not an independent
unbiased temporal dataset**. [Zenodo 22747162](https://zenodo.org/records/22747162).

## Archives and inaccessible operational histories

**Software Heritage:** origin visits/snapshots link to revisions, directories
and content-addressed blobs. That permits reconstructable history in principle,
but total archived software does not measure configuration-specific volume.
API index HTTP 200 is not recovered content; current documentation requests
encountered Anubis. Indexed primary docs describe versioned ORC/edge graph
exports, Athena access and requested graph RPC access. Athena is potentially
billable; no graph or blob extraction was run. Original source licenses remain.
[Data access](https://docs.softwareheritage.org/user/using_data/index.html),
[graph releases](https://docs.softwareheritage.org/devel/swh-dataset/graph/dataset.html),
[API terms](https://www.softwareheritage.org/legal/api-terms-of-use/).

**BigQuery:** primary codelab demonstrates commit analysis; historically
documented `github_repos` tables include commits, files, contents, licenses
and samples. Current anonymous table listing returned 401, so current table
inventory/size was not independently queried. Google's 2016 launch figures
are dated, not current population measurements. Discovery questions could be
counts/sizes of `.tf` or Compose paths and candidate sampling; that is not
full historical content recovery. Future queries require dry run and
`maximum_bytes_billed`; the published free tier is first 1 TiB/month, with
regional on-demand pricing thereafter. No billing action was taken.
[Official codelab](https://codelabs.developers.google.com/codelabs/bigquery-github),
[pricing](https://cloud.google.com/bigquery/pricing),
[cost controls](https://docs.cloud.google.com/bigquery/docs/best-practices-costs).

**PeeringDB:** CAIDA describes SQL/SQLite v1 from 2010-07-29 to 2016-03-13,
SQL v2 from 2016-05-27 to 2018-03-10, JSON from 2018-03-11 onward. The
transition gap, schemas, IDs, changed field sets and daily completeness must
be evaluated after permitted download. The archive endpoint returned 401;
no request form was submitted. Contacts/PII must be excluded, and changing
`updated_at` alone is not substantive policy evolution. This is a valuable
blocked alternative, not a successfully probed shortlisted source.
[Catalog](https://www.caida.org/catalog/datasets/peeringdb/),
[request](https://www.caida.org/catalog/datasets/request_user_info_forms/peeringdb/),
[acceptable use agreement](https://www.caida.org/about/legal/aua/caida_pub_aua.html).

**Two Campuses:** the IMC 2011 paper *The Evolution of Network Configuration:
A Tale of Two Campuses* concerns Georgia Tech and UW Madison, with five years
of RANCID/CVS/RCS histories. Table 1 totals are respectively 1,097 and 1,624
devices; observation/commit schedules differ between institutions/device
types. Public raw histories or supplementary raw downloads were not found.
It is academic evidence only, not a currently ingestible corpus. No authors
were contacted. [Primary paper](https://pages.cs.wisc.edu/~akella/papers/uw-gatech-config.pdf).

## Actual operational probes

| Source / unit | Measured entities | Records/observations | Actual differences/events | Logical bytes | Source-time window | Natural rate |
|---|---:|---:|---:|---:|---|---|
| RPKI / anchor-scoped authorization tuples | 367,204 union | 5,062,923 daily rows | 12,016 tuple additions/removals | 909,372,097 CSV | 2026-09-01–14 | N/A: daily observation censoring |
| DNS / query names, day one | 172,474 | 2,234,863 two-day rows | 4,091 changed common A/AAAA groups | 252,307,526 uncompressed columns | 2026-09-01–02 | N/A: observations, not event stream |
| RIS / prefixes, peer-specific events | 211,670 prefixes / 98 peers | 2,300,578 MRT records | 4,725,156 parsed NLRI events | 427,068,595 MRT | 2026-09-01 00:00–01:00 UTC | 1,312.54 events/s |
| ZoneStream / messages | N/A | 0 received in two sessions | N/A population rate | 0 payload bytes | Two bounded 30s sessions | N/A, not proven zero |
| PeeringDB / historical entities | N/A | N/A | N/A | N/A | Not probed: access 401 | N/A |

### RPKI results and long-history cautions

Compressed download 63,816,988 bytes; 14 usable dates, zero duplicate tuples
within dates, no missing/corrupt probe dates. Daily population grew from
360,226 to 362,402 tuples; maximum one-day additions/removals was 1,408;
maximum changes associated with one ASN was 382. Initial fetch/parse took
101.32 seconds. The 5,062,923 repeated observations are **not** five million
policy changes. Daily results include unchanged counts, ASNs, prefixes and
change ratios in [rpki.json](../workloads/results/dataset-feasibility/rpki.json).

RIPE-anchor directory listings show years 2011–2026; the earliest day directory
in January 2011 is **2011-01-21**, and October 2026 listings reach **2026-10-11**.
These are listed date boundaries, not validation of every snapshot or all
trust anchors. Archive-wide daily completeness was not verified.
The publisher documents missing/partial files and processing changes:
JSON additions, compression/schema changes and validator effects. We did not
join all years or assert global anchor completeness.
[Archive README](https://ftp.ripe.net/rpki/README.txt),
[maintenance history](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md).

### DNS results and ambiguity

The two full `.li` partitions total 98,828,670 compressed Parquet bytes.
Daily rows: 1,117,660 and 1,117,203; query names: 172,474 and 172,318.
Among 144,883 common successful address groups, 4,091 changed (2.82%):
7,716 added values, 7,612 removed, 168,436 unchanged. There were 485 newly
observed and 676 no-longer-observed groups. Canonical address JSON was
20,140,457 bytes; this is a different unit from full uncompressed columns.
Selected data parsed in approximately 6.7 seconds. Distribution and timestamp
ranges are in [dns.json](../workloads/results/dataset-feasibility/dns.json).

The project documents daily measurements since 2015; `.li` since 2020-05-19.
Open ccTLD/top-list data and closed major zones have different access policies.
Headline global daily figures are publisher statements, not our measured
population. Consecutive observations can differ from measurement variability,
load balancing and resolver behavior, so **no authoritative-zone edit count**
was inferred. Noncommercial CC-BY-NC-SA-4 conditions and explicit terms apply.
The research leading to these results was made possible by OpenINTEL
(https://www.openintel.nl/), a joint project of the University of Twente,
SIDN, NLnet Labs and SURF. Related measurement work:
[2016 DNS study](https://doi.org/10.1109/JSAC.2016.2558918).

### ZoneStream

Official docs expose Kafka port 9091 and WebSocket `/ws/zone_diff`, with DNS
schema-based records; certificate-transparency topics have different meaning.
Two public WebSocket connections completed without errors but zero messages
arrived. Live transport accessibility, received event schema and historical
replayability therefore remain separate findings. It stays secondary, not
a measured velocity winner. [Official docs](https://openintel.nl/data/zonestream/),
[DarkDNS research](https://doi.org/10.1145/3646547.3689021).

### Routing events

RIS archives: 73,771,296 compressed bytes, 427,068,595 uncompressed MRT bytes.
2,291,545 UPDATE messages expand into 4,462,391 announcements and 262,765
withdrawals: 4,725,156 prefix events. Peak minute 181,868 events. There were
118,172 duplicate fingerprints, no observed out-of-order message times,
no missing files and 57 excluded parser errors. Corrected cached parse took
98.73 seconds. No replay rate was substituted for source-time velocity.
[bgp.json](../workloads/results/dataset-feasibility/bgp.json).

RIS `rrc00` listing begins at 1999.09, longer than the selected new state
sources; collector histories/gaps and peer populations differ. RouteViews
main/`route-views2` month listings start at 2001.10. They also contain a
future 2026.11 directory: directory existence alone is not available data.
The [bounded coverage listing record](../workloads/results/dataset-feasibility/coverage-listings.json)
distinguishes this from verified source timestamps.
RouteViews
is a valuable independent observation alternative, but selecting both adds
less semantic variety than DNS or policy state. BGPStream provides access to
RIS/RouteViews, not independent copies of the same observations. Source
licenses/terms must be carried separately from the MIT parser's license.
[RIS docs](https://www.ris.ripe.net/docs/mrt/),
[RouteViews archive](https://archive.routeviews.org/),
[BGPStream providers](https://bgpstream.caida.org/data),
[RIPE copyright terms](https://www.ripe.net/about-us/legal/copyright-statement/).

## Main interpretation

The strongest direct configuration semantics remain observed Terraform file
history, but it remains sparse. RPKI offers operational authorization-state
semantics, DNS offers large repeated observed state, and RIS offers **natural
high-velocity routing events**. They cannot be added up as configuration edits.
The accessible historical source with the longest verified directory span in
this survey is RIS `rrc00`; no claim of gap-free history is made. PeeringDB
would improve long-term peering evolution if legitimate access succeeds.

Artifact metadata is easiest to download but does not replace actual contents.
Existing Terraform snapshots are cheapest to reuse without semantic changes.
Batfish Apache-2 licensed static fixtures are among the clearest legal baseline
sources; no large operational source is declared universally unrestricted.
For the next phase use [the evidence-based shortlist](phase13-shortlist-decision.md),
not the largest headline count.
