<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Quick Start

This walkthrough uses Kafka GitOps **0.4.0**, **Java 21** and an existing local
Kafka cluster. It creates or updates a sample topic with ACL management skipped
and deletion disabled. Use a non-production cluster and review the actual plan.

## 1. Download the JAR

Download `kafka-gitops-all.jar` from the
[v0.4.0 release](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0).
Verify its SHA-256 using [installation instructions](installation.md), then run:

```bash
java -jar kafka-gitops-all.jar --version
```

The version must be `0.4.0`. A separate `kafka-gitops` launcher installation is
not required. See the [English](release/kafka-gitops-v0.4.0-release-notes-en.md) /
[Russian](release/kafka-gitops-v0.4.0-release-notes-ru.md) notes for compatibility.

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
