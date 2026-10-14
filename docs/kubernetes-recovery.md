# Recovery boundary

Actual pod-restart/checkpoint/application recovery tests on Kubernetes are
**NOT_RUN**. Prior local Phase 17–19 recovery remains valid within its own
environment and cannot be relabeled Kubernetes recovery.

## Required owned-fixture campaign

After resource authorization and exact clean streaming/batch success:

1. Record Kafka selected offsets and accepted fixture identities.
2. Restart the owned Kafka broker pod without deleting its PVC; verify messages.
3. Restart owned MinIO without deleting its PVC; verify committed object digests.
4. Replay an identical source record: logical inventory unchanged.
5. Preserve distinct equal-value source identities.
6. Raise a callback exception after immutable commit and before checkpoint ACK;
   restart the same owned checkpoint, validate exact logical history.
7. Verify temporary uncommitted output is invisible.
8. Corrupt/remove only disposable owned fixture objects; require the declared
   committed integrity reader/independent validator to reject them.
9. Verify batch readback and search metadata/API projection after restart.

Never inject faults into earlier accepted datasets. Tests do not establish power
loss durability, multiwriter ACID, broker failover or node-loss durability.

## Format honesty

Existing configuration lake format 1.0 validates registered commit paths and
stores counts, not per-file cryptographic checksums. It must not be described as
having gained RIS's stronger physical inventory guarantees. A missing/corrupted
fixture must be independently checked; if a format repair is needed it requires
a narrow versioned amendment and compatibility tests before acceptance.

Native submission uses an owned single-node RWO checkpoint/evidence PVC visible
to driver/executor. That is not a portable multi-node checkpoint design.

The first image-build resource failure is retained in
[resource failure](../workloads/results/phase20-k8s/resource-failure.json). No source
events, analytical completion descriptor or false checkpoint success was published.
