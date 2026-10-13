# Phase 19 — DNS uncertainty and E4

The frozen role is **MeasurementObservation**. A complete selected Parquet object
does not establish successful measurements of every `.li` entity, every daily
partition, a complete answer, an authoritative zone or a cause of observed change.

## States and time

Status 0 is a successful source measurement. Only eligible A/AAAA response values
form the address projection. Successful non-address rows remain raw evidence,
not errors. Dictionary status 65535 is timeout. RCODE 2/3 are unsuccessful evidence.
Selected objects contain 65533, not 65535; its undocumented meaning remains
**unknown**, with the exact numeric code retained. Missing addresses, failure,
timeout and an absent measurement never become an empty set or deletion.

No real `proven_empty` is inferred from these response-row objects. Synthetic
owned fixtures may supply explicit complete-query NODATA evidence through the
Phase 16 proof-bearing model. Its constructor rejects fake empty success.
Absent coverage is a derived fact, never a synthetic raw Parquet coordinate.

Timestamp integers encode epoch milliseconds, while source measurement precision
is **second**, per the [dictionary](https://openintel.nl/data/dictionary/).
Both are retained. Unknown times are tagged unknown, never filled with processing
time. A group records its actual contributing min/max timestamps, not one invented
daily instant. Worker IDs are recorded nodes, not proof of a stable resolver or
network vantage. Multiple workers remain multiple contributors; no fictional worker.

## Two comparison policies

1. **Phase 13 descriptive:** four-field key `(query_name, query_type,
   response_name, response_type)`; lowercase / trailing-dot removal; status 0 and
   family-valid A/AAAA response address. Query type is not restricted to A/AAAA:
   eighteen AFSDB-query/A-response rows per day are part of the frozen projection.
   Compare observed value sets, never label them confirmed configuration edits.
2. **Strict matched-vantage qualification:** successful source/group evidence,
   compatible method/policy/time/worker and independently established vantage.
   The selected real data does not establish resolver/vantage or method version.
   Hence zero groups prove strict matched-vantage eligibility. The common groups
   have unknown qualification, not proved equivalence. Equal recorded worker-set
   sensitivity is reported separately and does not repair this missing evidence.

Newly/no-longer-observed groups describe selected-data availability. Removed
observed values describe set difference only. Neither means authoritative deletion.

## E4 counterexamples

Owned tests independently specify missing partition/chunk/measurement, timeout,
failure, nonempty success, proved/fake empty, changed/unknown/multiple workers,
method changes, equal values and different values, precision mismatch, source
retries/conflicts, partial completion and corrupted group/value references.
An unsafe fixture control maps missing input to removal; the safe qualification
path rejects it. Equal values at distinct original coordinates remain two rows.
All source rows are accounted for separately from the analytical address subset.

Three barriers remain distinct: selected-source integrity/coordinate completeness;
measurement/group evidence completeness; comparison eligibility. A complete first
barrier does not prove the second for all entities or the third for any vantage.
This counterexample, not merely a changed-IP count, is the E4 generalization test.
