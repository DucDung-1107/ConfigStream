# P16-2026-10-12-DNS-OPTIONAL — 2026-10-12

Starting commit: `362fb7cbee02258456ca7ed3e9042b6d720ae8bd`.
This controlled amendment corrects a dependency defect, not an empirical finding.
Phase 15 documents/results remain historical and unchanged. One problem, three
RQs, four desired roles and prospective K1–K3 remain frozen.

Old rule: W10/W11 require W9 and Phase 20 requires Phase 19, despite optional DNS.
New rule: recovery/publication/baseline tests require all retained accepted kinds.
Shared publication and Terraform compatibility always run. Track F includes DNS,
W8/W9 and E4; Track R excludes W8/W9 but retains Terraform/RPKI/RIS correctness,
recovery, baselines and evaluation. Phase 20 in R requires 16/17/18, not 19.
Track R cannot claim DNS generalization or four-role K3 validation.

The [machine-readable graph](../workloads/results/phase16/dependency-amendment.json)
is authoritative for future dependency closure and tested for cycles/dangling
dependencies in both tracks. This is a static specification, not a workflow engine.
Owner authorization covers this reversible service-free foundation only; instructor,
labor-budget and source-publication approvals are unresolved. No Phase 17 or later
work is implicitly authorized. DNS-specific legal/oracle/resource failure selects
Track R without weakening acceptance for its retained sources.

## P16-ID-v1 — identity elaboration, same date

Before new source ingestion, v1 binds parser, canonicalization and counting-policy
versions into the identity policy namespace. Phase 15 specified policy-versioned
identity but its illustrative RIS preimage named counting policy only. The
elaboration ensures a parser/canonicalization change cannot silently reuse old
identities. Coordinates remain artifact hash + decompressed offset + expansion
ordinal; all-record index remains audit metadata, not a fallback. Original
configuration IDs and Phase 13 counts are unaffected. Tests require all three
policy changes to yield a new ID. This decision authorizes the foundational
definition only, not any new real-data counting policy or pipeline.
