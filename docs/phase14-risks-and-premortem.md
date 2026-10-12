# Phase 14 — red team, legal gates and pre-mortem

Scope under review: two principal new domains, **RPKI policy observations and RIS
routing occurrences**, not a universal configuration platform. See
[architecture](phase14-architecture-options.md) and [decision](phase14-go-no-go.md).
Probabilities below are qualitative engineering judgments, not estimated rates.
Mitigation hours belong to the packages in [feasibility](phase14-feasibility.md),
not additional hidden implementation work.

## Ten objections that must survive review

| Objection | Severity / supporting evidence | Fatal? | Concrete mitigation / cost | Residual risk |
|---|---|---|---|---|
| 1. Four unrelated demos | High: four different identities, timestamps and meanings in Phase 13 | Fatal to full-scope coherence if only Kafka is shared | Reduce to RPKI/RIS; one preservation/cost protocol, common recovery suite; P0/P1/P8/P11 | Instructor may require a domain-specific operational result, not systems evaluation |
| 2. Kafka/Spark unnecessary for most sources | High: 14 RPKI days are daily sets; Terraform has 10,188 revisions | Not fatal if comparative evaluation; fatal to necessity claims | Strong script/batch baselines; concede daily source does not require streaming; P7/P10 | Batch may be simpler and faster; report it |
| 3. One collector-hour proves no generality | High: only `rrc00`, one hour and 57 exclusions | Fatal to Internet-wide rates, not bounded capacity experiment | Scope claims to this input; optional second bounded window only after MVP/approval; P10 | No broad network/diurnal coverage; day-scale behavior unknown |
| 4. DNS differences aren't config edits | High: A/AAAA changes can reflect balancing/measurement | Fatal to authoritative-configuration claim | Postpone DNS; preserve its observation-only finding, no adapter now | State-history title would still need explicit measurement caveats if reintroduced |
| 5. RPKI diffs need only a script | High: tuple-set differences are simple and low-density | Not fatal to cost/reconstruction evaluation; fatal to novelty-by-diff | Compare identical projected snapshot/delta layouts and reconstruction; P4/P7 | Expected benefit may be mundane; no new algorithm claim |
| 6. Version processing cannot process BGP | High: config parser/version buffer is hardwired | Fatal to adapter-only design | Native occurrence path, no fake versions, additive typed lake registration; P1/P2/P5/P6 | Significant new work; prior 938 snapshot/s does not predict event capacity |
| 7. Unified model has too many exceptions | High: missing dates, observation uncertainty, event multiplicity differ | Fatal to universal payload/state machine | Shared provenance/identity/publication only; typed payloads, separate queries; P1/P8 | If common contract becomes mostly optional fields, stop generic abstraction |
| 8. Access/license complicates reproduction | High: artifact != source licenses; OpenINTEL NC-SA; RIPE attribution/permission conditions | Fatal to unauthorized raw publication, not metadata-only review | Cache-only inputs, source manifests, owned CI fixtures, legal approval before data distribution; P0/P11/P12 | Future access/terms may change; no raw/normalized sample grant inferred |
| 9. Complexity without contribution | High: Spark, BGPStream, event sourcing and temporal lakes already exist | Fatal to new-platform novelty claim | Publish reproducible semantic failure cases and measured storage/query trade-offs; P7/P10/P11 | If only tool demo survives, fall back to existing architecture report |
| 10. Evaluation is counts/throughput only | High: aggregate counts can conceal lost/extra identities | Fatal to claimed history correctness | Coordinate multisets, set digests, failure injection, historical reconstruction and query baselines; P5/P7/P8/P10 | Exact oracle maintenance and parser exclusions remain substantial work |

No objection is dismissed by saying there are many Big Data “Vs”. Several are
fatal to particular claims even if the reduced implementation remains worthwhile.

## Table E — operational risk register

| Risk | Probability | Impact | Mitigation / early signal | Residual |
|---|---|---|---|---|
| Scope creep into ROV/hijacks/full route-state | High | Very high | Explicit no-join/no-RIB-state scope; a request for a prefix-policy join triggers new approval | Medium |
| Event multiplicity lost by payload/time dedup | Medium | Very high | Distinct-coordinate identical-event fixture; exact multiset oracle before services | Low after acceptance |
| False removals from incomplete daily states | Medium | Very high | Completion counts/hash + missing-day barrier; refuse transitions from partial input | Low-medium: source validation can still be incomplete |
| Shared lake refactor breaks config recovery | Medium | High | Additive registry; old manifests/checkpoints unchanged; run existing regressions | Medium until restart tests |
| MRT unsupported/error records hidden | Medium | High | Preserve 57-error baseline; safe category/coordinate ledger; independent fixtures | Medium: parser is not guaranteed complete |
| Encoding/state/small-file pressure | Medium | High | Five-minute pilot, explicit bytes/file/RSS metrics; 50 GiB total/20 GiB free guard | Medium; no capacity measurement yet |
| Weak academic distinction | Medium-high | Very high | Instructor sign-off on empirical systems RQs, strong batch/storage baselines | Medium; student contribution, not publication novelty |
| Legal/redistribution ambiguity | Medium | High | No external raw/normalized data in Git/demo; legal provenance, aggregates and owned fixtures only | Medium; grants require clarification before expanded publication |
| Unavailable inputs/changed terms | Medium | Medium | Freeze hashes and exact URLs; private cache reuse, fail visibly without replacement | Medium |
| Time estimates underestimate integration | Medium-high | High | 110/194/346 h budget including tests/report; milestones with kill criteria | Medium-high; no deadline/budget supplied |
| Biased source coverage | High | High for generality | Label one anchor/collector and static population/sample differences | High for global claims, acceptable for bounded evaluation |
| No measurable advantage over batch | Medium | Medium | Report negative cost results; do not equate lack of speedup with incorrectness | Low if instructor accepts evaluation |

## Five-cause pre-mortem

Assume expansion failed at presentation. Prevent the following now.

| Plausible cause | Early warning | Earliest detection point | Preventive action | Fallback |
|---|---|---|---|---|
| Scope overrun | Adapter plan gains DNS, RIB, routing validation, new API or cluster deployment | Instructor/contracts gate | Freeze two principal sources; fund tests/experiments before features; reject unbudgeted additions | Present Phases 0–13, or completed RPKI-only state experiment with narrower title |
| Incoherent research framing | Presenter says “all are config changes”, or cannot state a shared falsifiable RQ | P0, before coding | One source-occurrence preservation and retention/query-cost problem; typed semantics table | Retain configuration-history architecture evaluation; change topic only if course rejects it |
| Wrong temporal identity/completeness | Different BGP records disappear; partial day emits removals; hashes/counts disagree | Offline P1/P5 fixtures before services | Distinct source coordinates; explicit complete-set barriers; exact identity oracle | Stop at validated data-inspection results; no performance claims on incorrect data |
| Evaluation offers no insight | Only row counts and a fast replay chart exist; no batch baseline or reconstruction | P7 experimental design, before expensive runs | Predeclare queries/layouts/failure cases; include negative results; compare within domain | Invest remaining hours in existing Phase 12 storage/correctness analysis rather than a new source |
| Runtime/legal reproduction fails | 5-minute input exceeds byte budget, repeat logs contain payloads, no data-use publication policy | P0 legal gate and first source pilot | Cache-only raw data, aggregates/owned fixtures, 15 GiB run/50 GiB total budget, failure ledger | Use already legal/reproducible baseline; omit raw demonstration; stop expansion without hiding failure |

## Access, security and legal gates

This is a conservative research/publication policy, **not legal advice or a blanket
license conclusion**. Primary terms were revisited on 2026-10-12. The repository
license does not sublicense external records. Public access does not authorize
arbitrary redistribution or commercial demonstrations.

| Source | Research use / attribution evidence | Raw/normalized redistribution | Public demonstration and Git policy |
|---|---|---|---|
| TerraDS / existing Git sample | [v2 artifact](https://zenodo.org/records/20339474) CC-BY-4.0; separate pinned upstream root license evidence in [Phase 12 acceptance](phase12-acceptance.md) | Artifact license does not license historical/vendor files; root-license screening is not every-old-file adjudication | Reuse existing filtered summaries privately; no raw configs in Git/API/demo; preserve source licenses and whole-history secret exclusions |
| RIPE RPKI archive | [Archive description](https://github.com/RIPE-NCC/internet-dataset-descriptions/blob/main/rpki-repo-archive.md); [RIPE copyright statement](https://www.ripe.net/about-us/legal/copyright-statement/) describes attributed research/educational/noncommercial use with conditions | No verified unrestricted open-data license for transformed archive records established in this review; unchanged-material condition and third-party rights require care | Publish aggregate methodology/counts with attribution; no raw or canonical tuple samples until permission/terms applicable to that distribution are documented |
| RIPE RIS | [MRT documentation](https://www.ris.ripe.net/docs/mrt/) describes collected peer measurements; RIPE general terms apply, and [commercial-use conditions](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/commercial-use/) impose additional requirements for paid derivatives | Research access is not an unconditional redistribution/marketing grant; commercial rules must not be silently imported as an academic-data license | Source pointers/hashes, own fixtures and aggregate statistics only in Git; preserve credit; no public raw peer-prefix sample or commercial demo assumed |
| OpenINTEL, postponed | [Terms](https://openintel.nl/download/terms/), dated 2024-12-11: CC-BY-NC-SA-4.0, required attribution/paper citation, bounded download usage | Noncommercial/share-alike conditions, prescribed acknowledgment and full terms accompany redistributed parts; do not treat derived record sets as unrestricted | Do not introduce DNS publication into MVP. Any later use must carry the publisher-prescribed acknowledgment and terms, cite the DNS paper, and resolve derivative-data obligations before raw/normalized samples are published |

OpenINTEL measurements discussed here come from the existing Phase 13 summaries.
Credit belongs to **OpenINTEL, University of Twente, SIDN, NLnet Labs and SURF**;
the publisher's exact acknowledgment is linked in its
[attribution notice](https://openintel.nl/download/terms/), and the required
research reference is [van Rijswijk-Deij et al., 2016](https://doi.org/10.1109/JSAC.2016.2558918).
Do not claim this review resolves every future publishing obligation. Before a
paper/demo using data, attach the required publisher notice through the legal
review rather than inventing a replacement license.

RIPE Data Repository authenticated-service agreements are **not automatically
the anonymous FTP archive terms**. Likewise, legal terms for BGPStream's code
do not grant redistribution of RIS inputs. The RIPE copyright page timed out in
direct web opens but its primary indexed text was accessible; the commercial
page was likewise verified from the primary indexed page. These access issues
do not convert unknown permissions into approval. Preserve the applicable terms
snapshot privately before later ingestion/publication; request clarification if
needed, but do not contact publishers without user approval.

Raw peer IPs, prefixes, domains and authorization relationships are not credentials,
but can expose sensitive topology when joined. MVP publishes aggregates and safe
provenance only, not contacts, DNS TXT/SOA values or source payloads. Existing
Elasticsearch/FastAPI remain unchanged and metadata-only. Local raw caches/lake
remain private; logs must never echo unsafe source content or credentials.
External configuration is data only: no Terraform/Ansible/Helm/Compose/Kubernetes
execution. CI uses only project-owned fixtures and no live dataset requests.

**Legal gate:** private bounded research and compact aggregate review are feasible
under the documented conservative policy; redistribution of raw/normalized
external records remains **conditional/blocked pending explicit evidence**.
If the instructor requires an openly downloadable raw demonstration dataset,
resolve that permission before approving implementation, or retain owned fixtures
for the demo and report that limitation. Do not bypass it by calling public data
“open source”.
