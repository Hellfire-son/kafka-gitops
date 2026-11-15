<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Kafka GitOps 0.4.0 documentation

Manage Apache Kafka topics, configurations and ACLs from a version-controlled
YAML desired state. This is the independent
[Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops) fork,
based on joschi/kafka-gitops 0.3.1 and originally created by Shawn Seymour.

**[Download v0.4.0](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0)** ·
**[English Release Notes](release/kafka-gitops-v0.4.0-release-notes-en.md)** ·
**[Документация на русском](release/kafka-gitops-v0.4.0-release-notes-ru.md)**

## Start here

1. [Install the universal JAR](installation.md), verify its checksum and select
   **Java 21**. The JAR includes **Kafka Client 4.4.0**.
2. Follow [Quick Start](quick-start.md) to prepare client properties and YAML,
   validate locally, generate a read-only plan, review it and apply explicitly.
3. Read [permissions](permissions.md), [services](services.md) and the
   [desired-state specification](specification.md) before extending the scope.

## Plan and apply

Desired-state files belong in version control. Propose a state change, review
it, generate a plan against the intended cluster and inspect every operation.
Only an explicit `apply` changes Kafka. Planning never applies automatically.
A failure can leave partial changes; there is no automatic rollback.

| Area | v0.4.0 behavior |
|---|---|
| Topics | Creation, configuration additions/updates/removals and partition increases. |
| Replication | Deterministic RF increases/decreases, rack checks, fresh preflight and completion verification. |
| ELR | Enabled level 1 requires known empty ELR/LastKnownELR and healthy ISR; critical unknown evidence blocks RF execution. |
| Plans | Current/desired values, ordered replica assignments, warnings and blockers; blocked plans are not exported as executable JSON. |
| Configuration | `--command-config` file values override corresponding environment values; explicit bootstrap is required and sensitive diagnostic values are redacted. |
| ACLs | Existing application, Streams, Connect and user-role profiles; topic DESCRIBE remains opt-in. |

RF drift is planned by default. `plan --no-manage-replication` excludes it.
A decrease needs independent `apply --allow-replication-decrease` without global
`--no-delete`. The latter forbids RF decreases even with permission. Existing
JSON plans do not replace fresh safety evidence or permissions.

## Compatibility and evidence

The final JAR passed separate live smoke tests on Kafka 3.2.3 and 3.7.1
(plan, new topic, RF 1→2) and Kafka 4.4.0 (plan, new topic, RF 1→2→3→2 with
ELR enabled). Earlier broader integration trials covered topic/config/partition
operations and RF 1→2→3→2 on all three versions using the preceding artifact.

ADS 4.3.1 is **NOT TESTED LIVE**. Full SASL/TLS/Kerberos/OAuth and ACL enforcement
acceptance are incomplete; no vendor or production certification is claimed.
Managed Kafka/Confluent Cloud is not fully integration-accepted. See
[Confluent Cloud and legacy account integration](confluent-cloud.md).

Use the EN/RU release notes for the current release summary and
[final technical acceptance](release/v0.4.0-final-review.md) for exact test scopes.
Audit, design and testing reports describe their dated stages and artifacts;
their earlier blockers and preparation statuses are historical evidence.

## Project and security

- [Changelog](../CHANGELOG.md)
- [Security policy and private vulnerability reporting](../SECURITY.md)
- [Contributing](../CONTRIBUTING.md)
- [Project provenance](../CHANGES-FROM-UPSTREAM.md)

Copyright (c) 2020 Shawn Seymour. Project license:
[Apache License 2.0](../LICENSE). Bundled components retain their own
[licenses and notices](../legal/README.md). No official endorsement or support
by ASF, Arenadata, Confluent or the original maintainers is claimed.
