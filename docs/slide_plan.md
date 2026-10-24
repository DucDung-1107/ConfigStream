# ConfigStream checkpoint presentation plan

**Format:** 14 slides, 12 to 15 minutes  
**Language:** English on the slides and in the speaker notes  
**Audience:** technical reviewers who need to understand the problem, the evidence boundary, and the current limits

## The presentation hook

## Notation used in the method slides

`v1`, `v2`, and `v3` are illustrative producer-assigned integer versions. They are not literal constants in the implementation and they do not describe the full dataset. The code stores the version in `ConfigSnapshot.version` and applies the same rule to any integer sequence.

The code-backed example is an out-of-order arrival sequence:

```text
arrival:       v1, v3, v2
valid edges:   v1 to v2 and v2 to v3
invalid edge:  v1 to v3 as an adjacent streaming transition
```

The pure pair-comparison helper can compare supplied endpoints such as `v1` and `v3` and report `version_gap = 1`. The canonical streaming state machine does not emit that non-adjacent edge as if the missing `v2` had been observed. Keep these two boundaries separate when presenting the method.

Open with a concrete mistake, not with the system architecture:

> A configuration can arrive late and still be correct. If we compare states by arrival time, we can create a change that never happened.

Show three records arriving in this order:

```text
Producer versions:  v1         v2         v3
Arrival order:      v1         v3         v2
```

Then ask:

> Which transitions are real?

Pause for two seconds. The answer is `v1 to v2` and `v2 to v3`. Say explicitly: "These labels are a small example of the code's integer version field. The real method does not depend on the numbers 1, 2, and 3." This gives the audience a reason to care about identity, ordering, provenance, and recovery before any component appears.

## Visual system for the deck

- Use a warm off-white background, dark navy text, and one accent color per concept: teal for input, amber for validation, blue for temporal records, green for committed history, and muted rose for rebuildable views.
- Use one composition per slide. Prefer one diagram, one table, or one result group. Avoid grids of small UI cards.
- Use a large title at 32 to 38 pt, body text at 20 to 24 pt, and labels at 16 pt or larger. Remove text before shrinking type.
- Keep titles descriptive. Use a takeaway title only when the slide presents a supported result.
- Reserve the bottom 15 percent of each slide for a short speaker cue or source note. Do not place paragraphs inside diagrams.
- Reveal complex visuals in stages. First show the problem, then the valid rule, then the consequence.
- Keep accepted, pending, and not-claimed evidence visually distinct. Never use a red border to imply failure unless the slide is explicitly about a failed trial.

## Slide-by-slide plan

### 1. Title

**Title:** ConfigStream: Reconstructing Auditable Configuration History

**Subtitle:** Checkpoint presentation · HUST SoICT · 2026

**Layout:** Large title on the left. A small three-state motif on the right: `v1`, `v2`, `v3`, labelled `illustrative version IDs`. Keep names and supervisor information in one quiet footer line.

**Opening line:** “This project asks how a system can store configuration history without confusing arrival time with version time.”

**Timing:** 30 seconds

### 2. Problem

**Title:** The previous arrival is not necessarily the previous version

**Main visual:** A single wide timeline with two rows.

```text
Producer history:  v1 -------- v2 -------- v3
Arrival sequence:  v1 -------- v3 -------- v2
```

Under it, show two comparisons:

```text
Arrival-based comparison:  v1 to v3   X   v3 to v2   X
Version-based comparison:  v1 to v2   OK  v2 to v3   OK
```

**On-slide copy:**

- A late record can be valid.
- Arrival order cannot define history.
- A wrong pair can create a false diff or alert.

**Speaker script:** “The problem is not that the data arrived late. The problem is treating delivery order as producer order. ConfigStream must decide which states are allowed to be compared before it computes a diff.”

**Hook transition:** “That gives us the design question: what evidence must a system retain before it calls a transition real?”

**Timing:** 75 seconds

### 3. Why this matters

**Title:** A plausible diff can still be false

**Layout:** Four-step horizontal story, with one short phrase per step:

```text
late arrival  →  wrong pairing  →  false change  →  difficult audit
```

Add three small examples below the line: missing predecessor, duplicate record, projection updated before history commit.

**Speaker point:** “The same issue affects correctness, observability, and recovery. A search result may look useful while the authoritative history is still incomplete.”

**Do not show:** Kafka, Spark, or a full architecture diagram yet.

**Timing:** 45 seconds

### 4. Research questions and scope

**Title:** The checkpoint tests reconstruction, authority, and measurement

**Layout:** Three large numbered questions across the slide. Under them, one thin scope line.

1. **Reconstruction:** Can the system identify valid adjacent states under disorder, duplicates, bounded state, and restart-sensitive execution?
2. **Authority:** Can committed history remain independent from rebuildable search and API views?
3. **Measurement:** How should correctness and performance be compared when workloads use different evidence units?

**Scope line:** “Current evidence covers the tested prototype boundary. It does not establish HA, cluster-wide scalability, or end-to-end exactly-once processing.”

**Timing:** 55 seconds

### 5. Method overview

**Title:** Claim gates come before publication

**Visual:** Use the redesigned architecture figure at readable size: [configstream-platform-architecture.pdf](../doc/report/figures/configstream-platform-architecture.pdf).

**Presenter path:** Read the figure from top to bottom:

1. Four source types provide different evidence.
2. Each source passes its own claim gate.
3. Each gate emits a typed temporal record with identity and provenance.
4. One commit writes Parquet datasets and a manifest.
5. Analytics and serving read the committed history and can be rebuilt.

**Speaker point:** “The shared point is the commit boundary. The temporal rules remain workload-specific.”

**Timing:** 75 seconds

### 6. Configuration reconstruction

**Title:** The buffer waits for the missing predecessor

**Layout:** Use one large horizontal state diagram for a single asset, `router-07`. Keep the diagram as the main visual and place only three short method labels underneath: `identity`, `ordering`, `emission`.

```text
receive v1  ->  receive v3  ->  hold both  ->  receive v2  ->  emit two valid pairs
```

Show the buffer state below each step. Put diagnostics in a narrow footer: `gap`, `duplicate`, `conflict`, `eviction`.

**Detailed method:**

1. Bind every snapshot to an `asset_id` and a producer version before comparing it.
2. Deduplicate by record identity before emission.
3. Sort retained records by producer version, not by arrival time.
4. Emit only when two records belong to the same asset and the successor version is exactly one greater.
5. Keep unresolved gaps, conflicts, late records, and evictions as diagnostics.

**State example:** After `v1` and `v3`, the buffer contains both records but emits no adjacent pair. When `v2` arrives, the sorted buffer becomes `[v1, v2, v3]`, so the streaming state emits `(v1, v2)` and `(v2, v3)`. A retry of `v2` is ignored as a duplicate and recorded as a diagnostic.

**Formal rule:** Emit `(p, q)` only when `asset(p) = asset(q)`, `version(q) = version(p) + 1`, both records remain in the bounded buffer, and the pair has not already been emitted.

**Presenter animation:** Reveal the five states one at a time. Pause after `[v1, v3]` and ask, "Would you emit a pair here?" The answer is no. Reveal `v2`, then highlight the two valid pairs.

**Speaker point:** “The buffer does not invent a missing predecessor. It waits, emits a diagnostic, or evicts state according to the declared policy.”

**Timing:** 110 seconds

### 7. Workload-specific temporal evidence

**Title:** Each temporal lane has a different completion rule

**Layout:** Use three horizontal lanes. Every lane shows the same four fields: `source`, `identity`, `gate`, and `typed output`. This makes the differences easy to compare without a dense text grid.

| Workload | Evidence unit | Claim gate |
|---|---|---|
| RPKI | canonical coordinate row | inventory `1 to N` is complete before a transition |
| RIS | peer-prefix occurrence | repeated occurrences remain separate records |
| DNS | measurement group | coverage and vantage remain explicit |

**RPKI lane**

```text
source rows -> coordinate inventory -> complete 1 to N -> daily transition
```

- Identity: canonical coordinate and observation date.
- Gate: the expected coordinate inventory must be complete before comparing states.
- Output: an `RPKITransition` with identity, completeness result, date, and provenance.
- Failure meaning: an incomplete inventory becomes an incomplete observation, not a claimed daily change.

**RIS lane**

```text
MRT frame -> peer and prefix identity -> retain each occurrence -> RIS observation
```

- Identity: peer, prefix, observation time, and occurrence identity.
- Gate: preserve repeated peer-prefix occurrences instead of collapsing them into a set.
- Output: an `RISObservation` that keeps multiplicity and source provenance.
- Failure meaning: duplicate-looking occurrences may be evidence, so deduplication must not erase them.

**DNS lane**

```text
measurement rows -> address and vantage group -> qualify coverage -> DNS group
```

- Identity: measurement group, address, vantage, and observation time.
- Gate: retain coverage and vantage information even when either value is unknown.
- Output: a `DNSMeasurementGroup` with uncertainty fields and provenance.
- Failure meaning: an unknown coverage or vantage value remains explicit rather than being silently treated as complete.

**Gate drawing:** Use `1 to N` for RPKI, three separate occurrence dots for RIS, and two small fields `coverage = ?` and `vantage = ?` for DNS. Avoid long prose inside lane boxes.

**Shared versus different:** All three lanes create typed records with identity and provenance. Only the gate logic is workload-specific. Do not draw the configuration version buffer as if it processed RPKI, RIS, or DNS.

**Speaker point:** “These workloads share the idea of temporal evidence, but they do not share one universal notion of a record.”

**Timing:** 135 seconds

### 8. Publication and recovery

**Title:** Publication separates history from serving

**Layout:** Use a left-to-right publication diagram with one clear commit boundary. Keep the authoritative path in green and the derived serving path in muted rose. The audience should be able to answer one question by looking at the slide: "What can we rebuild if the index fails?"

```text
typed records
     ↓
Parquet datasets
     ↓
manifest with hashes, counts, and provenance
     ↓
analytics and rebuildable serving views
```

Use the green commit card as the visual center. Show the manifest as a gate, not as decoration. Its fields should be visible as four short labels: `query_id`, `file inventory`, `row counts`, and `SHA-256`. Put `search`, `FastAPI`, and `dashboard` in one muted rose box labelled `rebuildable projection`.

**Publication sequence:**

1. Write raw and processed records to the lake in Parquet.
2. Record the query or batch identity and the expected file inventory.
3. Validate file paths, byte counts, row counts, and SHA-256 values.
4. Publish the manifest only after validation succeeds.
5. Let analytics and serving readers consume committed history.

**Recovery sequence:**

```text
projection missing or stale
            -> read committed Parquet
            -> rebuild the projection
            -> compare IDs and counts
            -> restore the read view
```

The recovery path must point back to committed history, never to a partially written index. Use one vertical line from the commit card to a horizontal split. Do not use diagonal connectors or a floating API box.

**What the commit proves:** the reader can identify which files belong to a run, verify physical integrity, count the records, and trace them to source provenance. It does not make Kafka, Spark, MinIO, and Elasticsearch one atomic transaction.

**Failure explanation:** if the search or API projection fails, the historical Parquet data remains the recovery source. If manifest validation fails, the batch remains uncommitted and readers must ignore it.

**Speaker point:** “If a projection fails, rebuild it from committed history. The projection does not redefine the history.”

**Important caveat:** “The sinks are not one distributed transaction. The project therefore does not claim end-to-end exactly-once processing.”

**Timing:** 135 seconds

### 9. Evaluation design

**Title:** Every number has a workload unit

**Layout:** Use one editable table and one small callout.

| Workload | Unit | Accepted role |
|---|---|---|
| TerraDS | file version | end-to-end configuration-history replay |
| RPKI | canonical row or member | row-oriented processing and storage |
| RIS | peer-prefix occurrence | multiplicity-preserving temporal processing |
| DNS | measurement or grouped row | grouped temporal comparison |

**Callout:** “Do not pool rates across workloads. State the denominator, run boundary, and processing scope.”

**Timing:** 55 seconds

### 10. Result: TerraDS replay

**Title:** TerraDS replay preserved the tested history

**Layout:** Three large numbers across the top and one thin timeline below.

- **10,188** snapshots
- **3,683** adjacent pairs
- **68.742 s** completion including drain

Footer: **0 alerts · 0 diagnostics**

Timeline labels: startup, publish, drain. State the measured publisher rate separately from the end-to-end completion rate.

**Caveat on the slide:** “This run used a state cap of 75 and a long retention setting. It did not use the default 32-version and 10-minute values.”

**Speaker point:** “This is an accepted replay result under a defined setup, not a universal capacity claim.”

**Timing:** 55 seconds

### 11. Result: matched processing campaign

**Title:** The compared modes produced identical normalized rows

**Layout:** Three small horizontal panels, one per workload. Show median and range in seconds. Keep the same x-axis only if the scales remain legible.

**Evidence line:** `27 trials = 9 configurations × 3 fresh-process repetitions`

**Result line:** “All full normalized rows matched. B3 reported zero selected-offset lag.”

**Interpretation:** “These are normalized-core timings. They exclude production parsing, state finalization, and paced replay.”

**Do not claim:** that the fastest mode is the best architecture. The modes perform different amounts of work.

**Timing:** 65 seconds

### 12. Result: RPKI storage layouts

**Title:** Compact RPKI layouts reduced analytical bytes

**Layout:** Three horizontal bars labelled with exact bytes:

- R1: 47,756,423 bytes
- R2: 3,555,516 bytes
- R3: 6,954,890 bytes

Callouts: **92.55% reduction for R2** and **85.44% reduction for R3**, both versus R1.

Add a small note: “Queries remained exact within the tested query set.”

**Negative finding:** Put the initial SQLite thread failure in speaker notes or a small side note, not in the main visual unless the audience asks about failure evidence.

**Timing:** 55 seconds

### 13. Limitations and next steps

**Title:** The prototype is bounded by what has been tested

**Purpose of the slide:** Separate completed evidence, unresolved risks, and claims that are deliberately outside the current scope. Present the limitations as the boundary that makes the current results interpretable.

**Layout:** Use two large panels and one narrow bottom strip. The audience should first see the evidence boundary, then the work that would change it.

**Left panel — What the current evidence supports**

- The configuration replay reconstructs adjacent producer-version pairs under the tested disorder, duplicate, gap, and bounded-state rules.
- The four evidence lanes produce typed records with explicit identity and provenance.
- Committed Parquet data and its manifest provide the authoritative recovery source for downstream projections.
- The tested storage comparison reports measured byte differences among the RPKI layouts for the declared query set.
- The matched-processing campaign supports the reported normalized-core timing comparison, with its stated workload units and run boundary.

**Right panel — What remains unverified**

- **RIS under paced replay:** run the registered replay with peer-prefix occurrence multiplicity preserved and completeness reported at the declared run boundary.
- **End-to-end recovery:** interrupt publication or remove a serving projection, rebuild from committed Parquet, and check IDs, counts, and provenance.
- **Recovery under repeated faults:** test checkpoint restart, interrupted writes, stale manifests, and repeated projection rebuilds as one adversarial sequence.
- **Operational scale:** measure throughput, latency, and memory for a multi-node deployment or a substantially larger state space.

**Bottom strip — Not claimed in this checkpoint**

- high availability
- cluster-wide scalability
- distributed exactly-once processing

Say explicitly: "These are scope limits, not failed results. The current experiments do not provide evidence for these claims, so the report does not make them."

**Next-step roadmap:** Draw a short four-stage arrow below the panels:

```text
1. Freeze the evidence pack
        ->
2. Run the paced RIS replay
        ->
3. Execute projection and checkpoint fault tests
        ->
4. Rebuild the final tables, figures, and claims
```

Under each stage, add one concrete deliverable:

1. **Evidence pack:** configuration, commit, seed, workload unit, acceptance rule, raw logs, and manifest.
2. **RIS replay:** occurrence-preserving output, completeness diagnostics, runtime, and storage measurements.
3. **Fault tests:** failure point, committed-file state, rebuild result, ID/count comparison, and provenance check.
4. **Final synthesis:** update the paper and slides only from accepted artifacts; remove any claim not supported by the final evidence table.

**Speaker script:** "The next steps are not a general feature list. Each one answers a specific unresolved question. First we freeze the current artifacts so the comparison is reproducible. Then we run the missing RIS replay. Next we test whether a serving projection and a checkpoint can be recovered from committed history after interruption. Only after those checks will we decide whether the final paper can widen any claim."

**Visual rule:** Use solid green borders for accepted evidence, amber borders for pending tests, and a muted rose background for claims outside scope. Do not use a progress percentage. The date boundary is **25 October 2026**.

**Timing:** 90 seconds

### 14. Conclusion

**Title:** A history is trustworthy only when its transitions are explainable

**Layout:** Four words, large and widely spaced: `identity`, `order`, `provenance`, `recovery`.

**Closing message:** “ConfigStream makes valid transitions explicit, commits the evidence once, and keeps the remaining claims bounded by what the experiments actually tested.”

**Final question for discussion:** “Which missing experiment would most change your confidence in the current scope?”

**Timing:** 35 seconds

## Delivery notes

### How to create the hook

Do not begin with “Today I will present…” Begin with the out-of-order records. Let the audience answer the pairing question before revealing the system rule. The first three slides should feel like one short story:

1. Here is a record-ordering mistake.
2. Here is why the mistake matters.
3. Here is the research question that follows.

### How to explain diagrams

Use a fixed sentence pattern: "This input carries X. The gate checks Y. The resulting record preserves Z." Do not read every label. Reveal only the part of the figure needed for the current sentence.

### How to handle numbers

Always say the unit and boundary aloud: "3,683 adjacent pairs in the TerraDS replay," not "3,683 transitions." Say when a result is a component timing, a single-node result, or a pending experiment.

### How to sound natural

- Prefer "we compare adjacent producer versions" to "we perform temporal reconstruction."
- Prefer "the projection can be rebuilt" to "the serving layer is rebuildable."
- Prefer "this run supports a bounded claim" to "this demonstrates scalability."
- Avoid slogans, unexplained acronyms, and long sentences inside visuals.

## Appendix slides for questions

1. Kafka topics and `asset_id` partitioning
2. Version-buffer policy and diagnostics
3. Normalization and deterministic diff identity
4. Full timing table for the matched campaign
5. Storage manifest fields and recovery sequence
6. Dashboard screenshots and API projection
7. Reproducibility commands and environment metadata
