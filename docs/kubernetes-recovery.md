# Executed Kubernetes recovery

Only owned disposable prefixes were faulted; earlier accepted evidence and the
original VM were untouched. See [results](../workloads/results/phase20-k8s/closure-recovery-validation.json).

- Configuration callback raised after immutable commit, before checkpoint ACK:
  commit present, `commits/0` absent. Restarting the same checkpoint/query ID
  acknowledged the batch and reconstructed two versions/one adjacent diff.
- Whole source replay: six physical deliveries, still two logical versions/one
  diff. Kafka latest offset and Spark end/latest offset six; final selected lag zero.
- Safe Kafka/MinIO pod replacement preserved Kafka offset three and all twenty
  selected committed objects byte-for-byte SHA-256; PVCs were never deleted.
- ES pod replacement preserved API snapshots two/latest version two.
- Uncommitted cloned output invisible; missing Parquet rejected
  `AnalysisException`; corrupt Parquet rejected `Py4JJavaError`; conflicting
  immutable commit rejected HTTP 412. Accepted original history unchanged.
- Existing RPKI/RIS/DNS processors reconciled source replay. RIS additionally
  exercised commit-before-ACK restart and durable source exclusions/completion.

Intermediate failed jobs remain negative evidence, not counted as successful
jobs: entrypoint validation, cleanup RBAC, post-query UUID telemetry and Kafka
AdminClient lifetime. Recovery and successful exact readback are separate.

Legacy configuration format 1.0 stores registered paths/counts, not a new per-file
checksum inventory. The exact independently specified fixture validator protects
the declared projection; no format amendment is implied. Typed domain contracts
remain unchanged.

Guarantees cover single-writer callback exceptions and pod restarts, not power
loss, global ACID, multiwriter transactions, node loss or broker failover.
