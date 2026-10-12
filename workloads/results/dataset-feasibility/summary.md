# Phase 13 measured dataset feasibility

23 mandatory candidates; no architecture change. Null = not measured/not applicable.
The Phase 12 decision B remains unchanged for direct configuration histories.

## All-candidate inventory

| ID | Candidate | Temporal | Access | Fit | Decision | Score |
|---|---|---|---|---|---|---:|
| A1 | [TerraDS v2 static corpus](https://zenodo.org/records/20339474) | T0 | VERIFIED_ACCESSIBLE | F1 | BASELINE_ONLY | 73 |
| A2 | [TerraDS observed Git sample](https://github.com/kongwoang/ConfigStream) | T2 | VERIFIED_ACCESSIBLE | F1 | PRIMARY_CANDIDATE | 83 |
| A3 | [FRRouting and NGINX observed histories](https://github.com/FRRouting/frr) | T2 | VERIFIED_ACCESSIBLE | F1 | BASELINE_ONLY | 73 |
| A4 | [Batfish curated configuration baselines](https://github.com/batfish/batfish) | T0 | VERIFIED_ACCESSIBLE | F1 | BASELINE_ONLY | 58 |
| A5 | [IaC Bug Study (Ansible, Puppet, Chef)](https://github.com/gdrosos/iac-bug-study-artifact) | T1 | VERIFIED_ACCESSIBLE | F1 | SECONDARY_CANDIDATE | 68 |
| A6 | [Docker Compose evolution replication](https://github.com/SAILResearch/replication-21-ibrahim-dockercompose) | T1 | VERIFIED_ACCESSIBLE | F1 | SECONDARY_CANDIDATE | 61 |
| A7 | [Docker Compose EMSE archived repositories](https://zenodo.org/records/10648448) | T2 | VERIFIED_ACCESSIBLE | F1 | SECONDARY_CANDIDATE | 68 |
| A8 | [ETH automatically orchestrable GitHub project population](https://zenodo.org/records/7194189) | T1 | VERIFIED_ACCESSIBLE | F1 | LOW_PRIORITY | 61 |
| A9 | [Kubernetes security-manifest study (corrected identity)](https://vtechworks.lib.vt.edu/bitstreams/3567356d-63d6-4165-8f1d-f4ce6c5104a1/download) | T0, T1 | VERIFIED_ACCESSIBLE | F1 | LOW_PRIORITY | 56 |
| A10 | [Helm charts empirical replication](https://zenodo.org/records/7552697) | T1 | VERIFIED_ACCESSIBLE | F1 | SECONDARY_CANDIDATE | 61 |
| A11 | [PerfSPEC Kubernetes audit/event dataset](https://github.com/hkerma/kubernetes-event-dataset) | TX | VERIFIED_ACCESSIBLE | F3 | REJECTED | 51 |
| A12 | [Apache/MySQL user configuration corpus](https://github.com/tianyin/configuration_datasets) | T0 | VERIFIED_ACCESSIBLE | F1 | LOW_PRIORITY | 48 |
| A13 | [BPlan formal-verification artifact](https://zenodo.org/records/22747162) | T0, T1, TX | VERIFIED_ACCESSIBLE | F1 | ACADEMIC_REFERENCE_ONLY | 55 |
| A14 | [Software Heritage archive and graph services](https://docs.softwareheritage.org/) | T2 | VERIFIED_METADATA_ONLY | F4 | ACADEMIC_REFERENCE_ONLY | 57 |
| A15 | [BigQuery GitHub public dataset](https://codelabs.developers.google.com/codelabs/bigquery-github) | T0, T1 | VERIFIED_METADATA_ONLY | F4 | LOW_PRIORITY | 45 |
| B1 | [RIPE NCC RPKI historical VRP archive](https://ftp.ripe.net/rpki/) | T4 | VERIFIED_ACCESSIBLE | F2 | PRIMARY_CANDIDATE | 87 |
| B2 | [CAIDA PeeringDB daily historical snapshots](https://www.caida.org/catalog/datasets/peeringdb/) | T3, T4 | ACCESS_REQUIRES_REGISTRATION | F2 | ACCESS_BLOCKED | 67 |
| B3 | [Two Campuses operational configuration evolution study](https://pages.cs.wisc.edu/~akella/papers/uw-gatech-config.pdf) | T4 | RAW_DATA_UNAVAILABLE | F1 | ACADEMIC_REFERENCE_ONLY | 58 |
| C1 | [OpenINTEL daily DNS observations](https://openintel.nl/) | T3 | VERIFIED_ACCESSIBLE | F2 | PRIMARY_CANDIDATE | 83 |
| C2 | [OpenINTEL ZoneStream zone_diff](https://openintel.nl/data/zonestream/) | T5 | VERIFIED_METADATA_ONLY | F3 | SECONDARY_CANDIDATE | 60 |
| D1 | [RIPE RIS MRT routing events](https://data.ris.ripe.net/) | T3, T5 | VERIFIED_ACCESSIBLE | F3 | PRIMARY_CANDIDATE | 84 |
| D2 | [RouteViews MRT routing archives](https://archive.routeviews.org/) | T3, T5 | VERIFIED_ACCESSIBLE | F3 | SECONDARY_CANDIDATE | 74 |
| D3 | [CAIDA BGPStream framework and broker](https://bgpstream.caida.org/) | T3, T5 | VERIFIED_METADATA_ONLY | F3 | LOW_PRIORITY | 66 |

## Actual measured samples (not population estimates)

| ID | Unit | Entities | Records | Changes | Logical bytes | Natural events/s |
|---|---|---:|---:|---:|---:|---:|
| A1 | current .tf files | 1513378 | N/A | N/A | 2601501820 | N/A |
| A2 | file incarnations / Git adjacent pairs | 6505 | 10188 | 3683 | 12278085 | N/A |
| A3 | file incarnations / Git adjacent pairs | 1906 | 2525 | 619 | 1133998 | N/A |
| B1 | canonical authorization tuples / added+removed transitions | 367204 | 5062923 | 12016 | 909372097 | N/A |
| D1 | unique prefixes / peer-specific NLRI events | 211670 | 4725156 | N/A | 427068595 | 1312.5433333333333 |
| C1 | query names/day; changed A/AAAA groups among common groups | 172474 | 2234863 | 4091 | 252307526 | N/A |

## Per-candidate strength, limitation and effort

| ID | Temporal | Main strength | Main limitation / decision reason | Effort |
|---|---|---|---|---|
| A1 | T0 | Large real static population and syntax/size baseline | Full corpus already measured in Phase 12; no static-to-temporal inference | reuse |
| A2 | T2 | Direct configuration-history control and sparse temporal baseline | Already replayed and adjacency-validated; complements new policy/state/events rather than claiming natural high frequency | reuse |
| A3 | T2 | Historical cross-domain reference | Small sparse public history; retain negative evidence | reuse |
| A4 | T0 | Network syntax correctness and controlled mutation baseline | Static educational fixtures, not natural production histories | reuse |
| A5 | T1 | Real fix taxonomy; conditional before/after retrieval | Bug-selected, not unbiased frequency; content must be recovered and checked separately | small bounded recovery |
| A6 | T1 | Longitudinal application configuration methodology | Useful evolution metadata but licensing and upstream recovery limit readiness | bounded upstream recovery |
| A7 | T2 | Recoverable archived VCS alternative | 39,003,853,032-byte mirror exceeds probe budget; random extraction from solid archive not established | high download; no mirror probe |
| A8 | T1 | Reproducible population discovery, not archived Compose contents | Larger population list alone adds no temporal contents beyond TerraDS methodology | metadata cheap; history mining new work |
| A9 | T0, T1 | Misconfiguration realism and corrected source attribution | Starting URL is not the claimed manifest-evolution study; no verified longitudinal raw corpus | small static artifact |
| A10 | T1 | Release evolution; content availability must be verified per version | Release metadata is richer than a static corpus but full historical chart contents not established | medium external recovery |
| A11 | TX | Real system instrumentation, not natural production velocity | Fails natural operational-data objective; processed events not full resource-state history | small parse |
| A12 | T0 | Real configuration-value realism | Small static samples, possible credentials/PII and unclear redistribution terms | small with security review |
| A13 | T0, T1, TX | Auxiliary methodology / selected case evidence | Not independent TerraDS population; documented five-module plans include manual modifications | small read-only inspection |
| A14 | T2 | Fallback archival recovery and research-scale discovery | Documentation access challenged by Anubis; anonymous API index reachable, historical content recovery unverified; graph RPC on request and Athena cost; no graph download | high bounded content recovery; no global query |
| A15 | T0, T1 | Population discovery / size estimates with dry-run and bytes cap | No paid query authorized; current full table sizes/coverage not verified; not historical blob source | account setup and query budget |
| B1 | T4 | Real authorization policy-state additions/removals over long retention | Actual contiguous policy-state probe; deduplicated tuples and validator-artifact caveat required | small state adapter in Round 2 only |
| B2 | T3, T4 | Long-term peering/infrastructure metadata evolution | Official historical download endpoint returns HTTP 401; form exists; no credentials supplied or bypassed | access approval then 7–14-day harmonization |
| B3 | T4 | Direct operational-config academic motivation | Paper is downloadable, raw histories are not established accessible; permission/author contact not attempted | permission-dependent, no probe |
| C1 | T3 | Natural observed DNS-state scale; measurement ambiguity explicit | Consecutive public partitions available; compare address sets only, not TTL/signatures/measurement metadata | moderate canonical state adapter in Round 2 |
| C2 | T5 | Near-real-time DNS zone events, distinct from daily observations | Two bounded 30-second WebSocket sessions connected without credentials but received zero messages; payload and historical replay remain unverified | bounded service test; history not established |
| D1 | T3, T5 | Natural high-velocity operational routing event workload | Measured real archive events; strong streaming evidence but not direct config changes | native event adapter/analytics in Round 2; snapshot state must not be forced |
| D2 | T3, T5 | Independent vantage-point robustness check | Operationally different views but redundant workload semantics with RIS; do not select both merely for size | same parser family, optional bounded second collector |
| D3 | T3, T5 | Tooling option, not independent evidence population | Double-counting underlying RIS/RouteViews events prohibited; optional parser/access tool only | native library installation may be nontrivial on macOS ARM |

## Full scoring comparison

All scores judgmental; other components and uncertainty are in scoring.csv.

| ID | Temporal /20 | Scale /20 | Access /15 | Fit /10 | Complementarity /10 | Total /100 |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 6 | 16 | 14 | 10 | 5 | 73 |
| A2 | 19 | 7 | 15 | 10 | 9 | 83 |
| A3 | 18 | 4 | 14 | 10 | 4 | 73 |
| A4 | 5 | 2 | 15 | 10 | 3 | 58 |
| A5 | 12 | 6 | 12 | 9 | 7 | 68 |
| A6 | 12 | 7 | 10 | 9 | 5 | 61 |
| A7 | 17 | 10 | 7 | 9 | 5 | 68 |
| A8 | 6 | 12 | 13 | 8 | 3 | 61 |
| A9 | 7 | 4 | 11 | 9 | 4 | 56 |
| A10 | 11 | 8 | 11 | 8 | 5 | 61 |
| A11 | 5 | 4 | 14 | 5 | 3 | 51 |
| A12 | 5 | 2 | 11 | 10 | 3 | 48 |
| A13 | 8 | 3 | 13 | 8 | 2 | 55 |
| A14 | 17 | 17 | 5 | 3 | 4 | 57 |
| A15 | 7 | 15 | 5 | 3 | 3 | 45 |
| B1 | 18 | 15 | 14 | 8 | 10 | 87 |
| B2 | 16 | 12 | 3 | 8 | 9 | 67 |
| B3 | 17 | 8 | 0 | 10 | 7 | 58 |
| C1 | 17 | 19 | 13 | 7 | 9 | 83 |
| C2 | 13 | 10 | 7 | 6 | 6 | 60 |
| D1 | 19 | 20 | 14 | 5 | 10 | 84 |
| D2 | 18 | 19 | 13 | 5 | 5 | 74 |
| D3 | 17 | 16 | 12 | 5 | 1 | 66 |

Preferred: **A2 + B1 + C1 + D1**; alternative: **A2 + B1 + C1**.
BGP events provide velocity, not proof of high-frequency configuration edits.
See the methodology and shortlist docs for filtering, parser failures and scope gate.
