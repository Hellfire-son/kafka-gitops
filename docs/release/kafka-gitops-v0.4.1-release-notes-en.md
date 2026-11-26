<!-- SPDX-License-Identifier: Apache-2.0 -->
# Kafka GitOps v0.4.1

Independent Final QA Round 3 passed. All four findings are **VERIFIED FIXED**.
The v0.4.1 JAR is **RELEASE READY**.

## Plan and apply output

- Detailed blocks cover proposed topic and ACL changes, warnings and blocked
  operations. Unchanged topics without diagnostics contribute to the summary.
  Existing INFO and `NO_CHANGE` messages remain available with `--verbose`.
- `CREATE` shows target partitions, replication factor and supplied configs.
  `UPDATE` shows old and new values for changes and identifies overrides that
  will be removed. Topic `DELETE` identifies the data deletion explicitly.
  ACL changes show the resource type and pattern, principal, host, operation
  and permission.
- RF changes retain ordered replica assignments, ISR, ELR, LastKnownELR,
  broker observations and reasons for blocking. Non-uniform RF retains the
  observed replica assignments for each partition.
- `SUMMARY` counts each changed topic once and lists ACL creates and deletes.
  It shows `SKIPPED` for scopes excluded with `--skip-acls` or `--skip-topics`.
  A blocked plan lists distinct blocked topics separately from diagnostic
  findings. A topic with a blocked operation is excluded from unchanged topics.
- Apply reports each outcome after the existing request or verification finishes.
  Outcomes distinguish success, partial completion, blocked or rejected work,
  unknown results, work that did not run, and verified no-ops. A JSON plan is
  insufficient to verify the total number of unchanged topics. The report shows
  `Unchanged topics: UNKNOWN` and lists explicit unchanged entries when present.

## Final QA fixes

- **QA-001.** ACL deletion uses the binding results returned by Kafka. Reapplying a saved
  deletion for an absent binding reports `NO_OP`, zero deletions and exit code
  0. When a later deletion fails, earlier confirmed deletions stay in the summary.
- **QA-002.** A partition plan that becomes stale before submission reports `BLOCKED`
  and exits with code 2. An unconfirmed submitted change reports `UNKNOWN`.
  Remaining operations are `NOT_RUN`. A stopped apply with earlier confirmed
  changes reports `APPLY_PARTIAL`.
- **QA-003.** The external IntegrationSpec suite uses independent temporary paths and
  normalizes line endings for exact transcript comparisons. It checks the
  current PLAN/APPLY contract, JSON and the observed Kafka state.
- **QA-004.** Final verification of an RF decrease can re-read metadata within its existing
  deadline when the ordered assignment matches the target, reassignment has ended,
  and the ISR contains the entire target plus only known removed replicas. Success
  requires an exact target ISR on every partition and fresh safety checks. Each
  Admin request and future wait uses the current time remaining in the same
  monotonic verification window.

```text
[PLAN] [CREATE] Topic: example-topic
  Partitions: 1
  Replication factor: 1
  Config cleanup.policy: compact
[SUMMARY]
  Topics: 1 to create, 0 to update, 0 to delete
  ACLs: 0 to create, 0 to delete
  Unchanged topics: 144
  Total resource changes: 1
  Status: PLAN_BUILT
  Ready for apply: FRESH_PREFLIGHT_REQUIRED
```

```text
[APPLY] [CREATE] Topic: example-topic
  Partitions: 1
  Replication factor: 1
  Config cleanup.policy: compact
  Result: SUCCESS
[SUMMARY]
  Topics: 1 created, 0 updated, 0 deleted
  ACLs: 0 created, 0 deleted
  Unchanged topics: 144
  Successful resource changes: 1
  Failed resource changes: 0
  Status: APPLY_COMPLETED
```

## Independent QA results

Independent Final QA Round 3 passed 1,153 JVM tests, 26 Python tests, 56 CLI
checks, 156 codec/JNI checks on each OS, and external IntegrationSpec on Java 21
(76/76 Windows, 76/76 Linux). The live matrix used the final v0.4.1 JAR.

| Broker | Verified local deployment |
|---|---|
| Apache Kafka 3.2.3 | ZooKeeper |
| Apache Kafka 3.7.1 | KRaft |
| Apache Kafka 4.4.0 | KRaft, ELR level 1 enabled |
| Confluent cp-kafka 7.9.11 | ZooKeeper, SASL/PLAIN and ACL enforcement |

Each platform covered Validate, read-only PLAN, topic and configuration changes,
partition increases, and RF 1 → 2 → 3 → 2. After each RF change, the ordered
replica assignments, full ISR and control records were verified. Confluent also
passed five RF 2 → 3 → 2 cycles and RF 4 → 6 on two topics across three racks,
with two replicas per rack. ADS, Kerberos, TLS and OAuth were outside the live
test matrix.

Deterministic probes verified the removed-replica ISR transition and rejection
of results at or after the deadline. The transient ISR did not occur naturally
in the live runs.

## Compatibility and safety

CLI flags, exit codes and the executable JSON schema remain compatible with
v0.4.0. `--include-unchanged` controls JSON export. `plan` reads Kafka state.
`apply` continues to enforce RF and broker safety checks, fresh preflight,
reassignment completion checks and the existing deletion and RF permissions.
Sensitive configuration values remain redacted in diagnostics. A blocked plan
preserves any existing JSON output file.

The universal JAR is built with Java 21, Kafka Client 4.4.0,
Gradle 9.9.1 and Shadow 9.6.1. Dependency versions and license notices are
preserved.

## Artifact and installation

The JAR requires Java 21. Verify the JAR and both notes using the release's
`SHA256SUMS`.

**JAR SHA-256**

```text
86d5228bc1b03e697fdab10cfa8bcb028a3ed9ef8304cd3f549e6c4acab89f05
```
