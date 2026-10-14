# Frozen IT4043E requirements

Audit baseline: main `0d395fbd377315d3e8fb053902818b7c5ce884a0`, clean and equal
to fetched origin/main. [Baseline CI](https://github.com/kongwoang/ConfigStream/actions/runs/38563761422)
passed. Source: [official instructor guideline](https://datalab.vn/teaching/it4043e-big-data-storage-and-processing),
read in full on 2026-10-14. The page labels the course **2020**; its site footer
update does not establish a current-semester syllabus.

## Teaching Schedule and administrative boundary

The published sequence covers Big Data overview, Hadoop, architecture/capstone,
HDFS, NoSQL, distributed messaging, Spark, Structured Streaming and analytics,
then presentations and wrap-up. It spans sixteen weeks. This is a historical
teaching sequence, not an independently verified schedule for this student's
semester. Dates, grading weights, group size, submission template/deadline and
presentation allocation are **UNVERIFIED**. No such values are invented here.

Teams access was attempted: the connector is not connected. A targeted search
of official HUST/SoICT pages did not identify the current course rubric; general
curriculum/prerequisite notices are not substitutes. Owner-supplied current
instructions can amend administrative fields separately. Default defense duration
is a project planning choice, not an instructor deadline.

## Objectives and General Requirements

| Requirement boundary | Interpretation | Expected evidence | Classification |
|---|---|---|---|
| Real-world Big Data problem | Explain measured workload shape, native units and validity | Accepted source ledgers/aggregate receipts | Mandatory |
| Lambda or Kappa Architecture | Actual executed paths, not just a labeled diagram | Native streaming and batch jobs, serving projection | Mandatory alternative |
| End-to-end pipeline | Ingestion, processing, storage and visualization connected | Owned Kafka/Spark/lake/API/dashboard demonstration | Mandatory |
| Apache Spark (PySpark or Scala) | Executed Spark computations | Tests, plans and Kubernetes executors | Mandatory |
| HDFS or equivalent | S3A/object-lake interface, explicitly qualified local persistence | Immutable Parquet readback/PVC evidence; no replication claim | Mandatory, equivalence needs instructor judgment |
| Distributed messaging | Kafka producer/consumer with offsets/replay | Strimzi/KRaft fixture receipts | Mandatory |
| NoSQL | Rebuildable Elasticsearch metadata projection | Mapping, rebuild and actual API query | Mandatory |
| Kubernetes or Cloud | Native Kubernetes scheduling, not Docker-only/local Spark | K3s node, driver/executor, namespace/PVC/RBAC | Mandatory alternative |

## Data Processing Requirements with Spark

Preserve the six official families: **Complex Aggregations**, **Advanced
Transformations**, **Join Operations**, **Performance Optimization**,
**Streaming Processing**, **Advanced Analytics**. They define intermediate-skill
coverage. Their listed techniques are audited individually, not silently waived.
The page does not explain how irrelevant MLlib/GraphFrames examples affect grading;
omissions remain documented and subject to instructor judgment.

- Complex Aggregations: windows, advanced aggregates, pivot/unpivot, custom aggregates.
- Advanced Transformations: staged/chained operations and actual Spark UDFs.
- Join Operations: broadcast, sort-merge and multi-join optimization.
- Performance Optimization: pruning, bucketing, caching/persistence and execution plans.
- Streaming Processing: modes, watermarks/late data, state and qualified recovery guarantees.
- Advanced Analytics: statistics/time-series, MLlib and GraphFrames applicability.

Small owned educational fixtures will close relevant missing demonstrations.
No fixture proves full-window performance, new scientific semantics, or end-to-end
exactly-once transport. Production defaults/data are not changed.

## Report Requirements

Four sections: **Problem Definition**, **Architecture and Design**,
**Implementation Details**, **Lessons Learned**. The final report uses these
as its four top-level parts, not a concatenation of phase histories.

Exactly eleven lesson categories: Data Ingestion; Data Processing with Spark;
Stream Processing; Data Storage; System Integration; Performance Optimization;
Monitoring & Debugging; Scaling; Data Quality & Testing; Security & Governance;
Fault Tolerance. Each has Problem Description, Approaches Tried, Final Solution
and Key Takeaways. Proposed production work is never labeled an executed solution.

## Closure policy

No public hosting, new datasets, private-source replay, destructive migration,
paid cloud or new architecture. Keep H3 NOT_RUN unless a separately approved
registered pacing experiment actually runs. Existing local K3s/dashboard and
accepted real-source scopes are reused; raw external rows and credentials remain
private. See [initial compliance audit](course-compliance-matrix.md).
