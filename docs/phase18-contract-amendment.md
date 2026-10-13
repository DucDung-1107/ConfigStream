# Phase 18 — controlled physical commit correction (2026-10-13)

Decision P18-M1: the first private five-minute pilot used experimental
`ris-1.0` metadata whose `provenance_ids` field incorrectly contained artifact
hashes. Source rows already carried the correct manifest ID, and exact oracle
comparison was enforced. That pilot subsequently failed report serialization
(Spark progress UUID), so it is **not accepted performance evidence**.

New writes use **`ris-1.1`**: separate `source_artifact_sha256s` and real
`provenance_ids`, with immutable pinned references published separately and
checked by readers. The physical row schema and `physical-v1` path are unchanged.
Legacy experimental `ris-1.0` reads retain their original checksum/oracle behavior;
they do not acquire the new metadata-binding guarantee retrospectively. No files
or commits from that attempt are rewritten. New runs have fresh isolated prefixes.

This correction changes neither Phase 16 evidence identity, MRT coordinates,
expansion order, parser acceptance, original fingerprint definition nor RPKI or
configuration contracts. Policy bindings remain `mrtparse-2.2.0`, `ris-wire-v1`,
`feasibility-v1`. Recovery, missing provenance and legacy commit tests are required.
It is a physical metadata amendment, not a new research question or data source.
