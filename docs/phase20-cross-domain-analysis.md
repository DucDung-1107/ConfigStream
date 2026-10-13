# Phase 20 — common principles, different semantics

| Role | Stable identity | Time | Completeness / reconstruction | Multiplicity / unsupported claim |
|---|---|---|---|---|
| Terraform VersionedConfiguration | accepted Git/blob/incarnation and ordinal snapshot ID | source Git timestamps + accepted ordinal; not deployment time | pinned accepted/censored history; normalized contents/edges | preserve versions; text-v1 is not semantic HCL truth or deployment history |
| RPKI StateObservation | source artifact/row separate from scoped canonical tuple and daily state | observation day, operation instant unknown | complete comparable selected states; exact baseline/delta canonical membership | repeated daily observations preserved; removals are not administrator commands |
| RIS EventOccurrence | compressed SHA + MRT offset + expansion ordinal + policy | original MRT precision, processing clock separate | framed/excluded/accepted ledgers; exact occurrence projection, not RIB | distinct equal fingerprints survive; parser disposition qualified, no incident causality |
| DNS MeasurementObservation | pinned Parquet SHA + original row-group/row + policy | epoch-ms encoding, second precision, group range | selected object complete != entity/answer complete; qualified groups/contributors | equal values do not erase rows; missing/timeout/failure != empty/deletion |

Shared I1–I9 concern auditable source identity, replay/conflict rejection, temporal
honesty, committed visibility and exact declared reproduction. Domain processors
cannot share one universal version/state/absence machine. Error ledgers are typed:
non-target DNS measurements are not parser errors, framed RIS exclusions are not
eligible events, incomplete RPKI states cannot create normal removals.

[DNS E4 acceptance](../workloads/results/phase19/acceptance.json) retains 144,883
common groups, 4,091 descriptive observed differences, 485 newly and 676
no-longer-observed groups. Strict matched-vantage eligibility remains **zero**;
all common groups carry unknown qualification. Original worker IDs are not proof
of stable resolver/vantage; no real proven_empty classification is fabricated.

K1 is scoped engineering correctness, K2 is measured local within-domain cost,
K3 is applicability **and limits** of common preservation principles. Four
adapters do not prove universal generalization. No pooled configuration-change
count, causal DNS edits, complete Internet routing state or horizontal scalability.

[Claim/evidence map](../workloads/results/phase20/claim-evidence-map.json) and
[unit registry](../workloads/results/phase20/unit-registry.json) identify supported
and unsupported interpretations. Primary costs include raw coordinate references,
full canonical/delta states, native occurrence payloads, measurement contributors,
broker retention, checkpoints, manifests and exact verification.
