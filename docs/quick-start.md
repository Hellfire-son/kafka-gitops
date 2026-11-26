<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Quick Start

This walkthrough uses Kafka GitOps **0.4.1**, **Java 21** and an existing local
Kafka cluster. It creates or updates a sample topic with ACL management skipped
and deletion disabled. Use a non-production cluster and review the actual plan.

## 1. Download the JAR

Download `kafka-gitops-all.jar` from the v0.4.1 entry under
[GitHub Releases](https://github.com/Hellfire-son/kafka-gitops/releases).
Verify its SHA-256 using [installation instructions](installation.md), then run:

```bash
java -jar kafka-gitops-all.jar --version
```

The version must be `0.4.1`. A separate `kafka-gitops` launcher installation is
not required. See the [English](release/kafka-gitops-v0.4.1-release-notes-en.md) /
[Russian](release/kafka-gitops-v0.4.1-release-notes-ru.md) notes for compatibility.

## 2. Prepare client.properties

For a disposable local PLAINTEXT lab, create `client.properties`:

```properties
bootstrap.servers=127.0.0.1:9092
security.protocol=PLAINTEXT
```

Set the actual bootstrap and required security properties for other environments.
Keep credentials outside Git. `--command-config` can supply all required Kafka
properties; `KAFKA_*` environment variables are optional. Explicit file values
override corresponding environment values. There is no implicit bootstrap server.
See [permissions](permissions.md) before using authenticated clusters.

## 3. Create the desired state

Create `state.yaml` in the working directory:

```yaml
topics:
  example-topic:
    partitions: 3
    replication: 1
    configs:
      cleanup.policy: compact
```

`state.yaml` is the default filename; root option `-f` selects another file.
See the [desired-state specification](specification.md) and [services](services.md)
for additional topics, service definitions and ACLs. Confluent Cloud acceptance
and its legacy account integration are discussed separately in
[Confluent Cloud](confluent-cloud.md).

## 4. Validate locally

```bash
java -jar kafka-gitops-all.jar -f state.yaml validate
```

This parses and validates the YAML without connecting to Kafka. Successful
validation returns exit code 0; invalid input returns a nonzero exit code.
It does not prove authorization, broker availability or sufficient RF capacity.

## 5. Generate a read-only plan

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete -f state.yaml plan -o plan.json
```

**`plan` only reads Kafka state; it never applies changes.** This example uses
`--skip-acls` to manage topics only. Global `--no-delete` disables deletion and
forbids RF decreases. Existing topics with the same names can still be updated.

The text report shows actual/desired values, proposed replica assignments,
config changes, warnings and blockers. Executable operations are saved as JSON
only when planning succeeds. A blocked plan returns exit code 2 and leaves an
existing output file untouched. Exit code 0 covers `PLAN_BUILT` and `NO_CHANGES`.

In v0.4.1, `CREATE`, `UPDATE` and `DELETE` label the topic
operations. ACL changes use `CREATE` and `DELETE`. `SUMMARY` reports counts by
resource and `ACLs: SKIPPED` for this example. Detailed blocks for unchanged
topics appear only when they have a warning or blocker. `--include-unchanged`
still controls which entries are written to JSON. `Blocked topics` counts
distinct topics and `Blocking diagnostics` counts individual findings.

## 6. Review before applying

- Confirm the target cluster and every resource in the plan. This walkthrough
  should contain only the intended topic operations and no ACL operations.
- Review old/new configuration values, partition counts and RF assignments.
  Resolve critical `UNKNOWN` evidence and blockers; assess operational warnings.
- Require a successful planning exit code. Do not apply an older JSON file as
  the output of a failed run. A saved RF plan still needs fresh apply checks.

## 7. Apply the reviewed plan explicitly

> **Warning:** `apply` changes Kafka. Without the example's guards, desired-state
> reconciliation can delete resources omitted from the state file. A failed apply
> can leave partial changes; there is no automatic rollback.

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete apply -p plan.json
```

In v0.4.1 the apply report uses the same operation labels as plan. `Result`
shows each resource's outcome. When a topic has several changes, the report
also shows the outcome of each step. `PARTIAL` means some work completed before
the resource was fully applied. `UNKNOWN` means the available evidence could
not establish the result, for example after a timeout. `NOT_RUN` marks operations
that were not reached. For saved JSON, `Unchanged topics: UNKNOWN` means that
the total was unavailable from the apply run.

The ACL deletion count comes from the bindings Kafka confirms it removed.
Repeating a saved deletion after the binding is gone reports `NO_OP` and exits
with code 0. If the partition count drifts from the saved plan before the write,
the partition step reports `BLOCKED`, later operations report `NOT_RUN`, and
the command exits with code 2. An unconfirmed submitted write reports `UNKNOWN`.

RF apply rechecks permissions, cluster/topic identity, assignments, broker/rack
health, leader/ISR, `min.insync.replicas`, ELR evidence and conflicting reassignments.
Success requires completed reassignment and independent verification, not just
an acknowledgement. Re-plan after completion or after investigating a failure.

## Replication options

RF drift is included in `plan` by default, including proposed decreases.

| Option | Meaning |
|---|---|
| `plan --no-manage-replication` | Exclude RF operations. |
| `apply --no-manage-replication` | Reject saved RF operations before any writes. |
| `apply --allow-replication-decrease` | Independently authorize an intended decrease after fresh checks. |
| Global `--no-delete` | Forbid RF decreases even with decrease permission. |

The old `--manage-replication` flag remains a deprecated compatibility alias;
opposite flags conflict. `plan --allow-replication-decrease` is only a compatibility
option and does not authorize apply. Partition decreases are unsupported.

Only for a separately reviewed RF decrease, without `--no-delete`:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls apply -p reviewed-decrease.json --allow-replication-decrease
```

`RACK_AWARE` is the default placement policy. The plan option
`--replication-rack-policy BROKER_ONLY` explicitly accepts loss of rack protection;
it does not waive the remaining checks. With ELR enabled, both ELR and LastKnownELR
must be known and empty; critical unknowns block RF execution. See
[ELR/RF design](design/elr-replication-compatibility.md) and the release notes.

## Inspect CLI help

```bash
java -jar kafka-gitops-all.jar --help
java -jar kafka-gitops-all.jar plan --help
java -jar kafka-gitops-all.jar apply --help
```
