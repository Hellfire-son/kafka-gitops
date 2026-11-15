<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Kafka GitOps

Declarative management of Apache Kafka topics, configurations and ACLs.

[![Latest release: v0.4.0](https://img.shields.io/badge/release-v0.4.0-2563eb?style=flat-square)](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0)
[![Java 21](https://img.shields.io/badge/Java-21-ea580c?style=flat-square)](docs/installation.md)
[![Apache License 2.0](https://img.shields.io/badge/license-Apache%202.0-475569?style=flat-square)](LICENSE)

**[English documentation](docs/release/kafka-gitops-v0.4.0-release-notes-en.md)** ·
**[Документация на русском](docs/release/kafka-gitops-v0.4.0-release-notes-ru.md)**

An independent community-maintained fork, distributed as one universal Java 21 JAR.

## Overview

Describe your Kafka topics and services in YAML and keep that desired state in
version control. Kafka GitOps compares it with the cluster through standard Kafka
Admin APIs and produces a plan of the required topic, configuration and ACL changes.

**Describe → Plan → Review → Apply.** Planning only reads cluster state. Changes
run through an explicit `apply`, with fresh safety checks for replication
operations. Service definitions generate ACLs using the existing profiles;
ACL/DESCRIBE semantics are unchanged.

## What's New in v0.4.0

Version 0.4.0 builds on the upstream 0.3.1 base:

| Area | Changes in this fork |
|---|---|
| Runtime and dependencies | Java 21, Kafka Client 4.4.0, Gradle 9.9.1 and Shadow 9.6.1; updated Jackson, logging and compression libraries. |
| Topics and configurations | Topic creation, config additions/updates/removals and partition increases. |
| Replication factor | RF increases and explicitly approved decreases, with deterministic replica placement and broker/rack checks. |
| ELR-aware safety | ISR, ELR and LastKnownELR evidence is checked; missing safety data is never assumed safe. |
| Plan output | Current and desired values, old/new config values, ordered replica assignments, warnings and blockers. |
| Apply and verification | Fresh RF preflight, checks between batches, reassignment monitoring and independent completion verification. |
| Connection diagnostics | Bounded waits and clear configuration, connection and metadata error categories. |
| Client configuration | `--command-config`, file-over-environment precedence and redaction of sensitive values in diagnostics. |
| Older Kafka clusters | One JAR with verified Kafka 3.x/4.x scenarios and capability checks for legacy metadata. |

See the [English release notes](docs/release/kafka-gitops-v0.4.0-release-notes-en.md)
for dependency versions, migration details and the complete scope of these changes.

## Compatibility

Earlier integration trials and final smoke tests of the released JAR are separate
evidence sets:

| Target | Earlier integration trials | Final v0.4.0 JAR smoke tests |
|---|---|---|
| Apache Kafka 3.2.3 | **Tested:** topics, configs, partition increases; RF 1 → 2 → 3 → 2 | **Tested:** plan, new topic, RF 1 → 2 |
| Apache Kafka 3.7.1 | **Tested:** topics, configs, partition increases; RF 1 → 2 → 3 → 2 | **Tested:** plan, new topic, RF 1 → 2 |
| Apache Kafka 4.4.0 | **Tested:** topics, configs, partition increases; RF 1 → 2 → 3 → 2 and ELR scenarios | **Tested:** plan, new topic, RF 1 → 2 → 3 → 2; ELR remained enabled at level 1 |
| Arenadata Streaming / ADS 4.3.1 | **NOT TESTED LIVE** | **NOT TESTED LIVE** |

The final Kafka 3.x smoke tests do not repeat the entire earlier matrix. See
[earlier integration results](docs/testing/kafka-3x-compatibility-results.md) and
[final technical acceptance](docs/release/v0.4.0-final-review.md) for exact scopes.
These results do not establish vendor or production certification. Compatibility
with old Kafka brokers is not a recommendation for new production deployments.

Final acceptance also recorded **1039 Java tests, 26 Python guard tests,
56 CLI smoke checks and 156 codec/JNI checks — PASS**, separately from live trials.

## Quick Start

Use an existing local Kafka cluster for this example. It manages a topic, skips
ACLs and disables deletion. Java 21 is required.

### 1. Download the JAR

Download `kafka-gitops-all.jar` from the
[latest release, v0.4.0](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0).
Compare its SHA-256 with the `SHA256SUMS` asset, then check the version:

```bash
sha256sum kafka-gitops-all.jar
java -jar kafka-gitops-all.jar --version
```

The version should be `0.4.0`. In PowerShell, use
`Get-FileHash .\kafka-gitops-all.jar -Algorithm SHA256` for the checksum.

### 2. Configure the Kafka client

Create `client.properties` for a local PLAINTEXT lab:

```properties
bootstrap.servers=127.0.0.1:9092
security.protocol=PLAINTEXT
```

For other environments, set the actual bootstrap addresses and required security
properties. Keep credentials outside Git. `--command-config` can provide all
required Kafka settings; no `KAFKA_*` environment variables are mandatory.
Explicit file values override corresponding environment values.

### 3. Describe the desired state

Save this as `state.yaml`, the default desired-state filename:

```yaml
topics:
  example-topic:
    partitions: 3
    replication: 1
    configs:
      cleanup.policy: compact
```

### 4. Validate locally

```bash
java -jar kafka-gitops-all.jar -f state.yaml validate
```

Validation checks the desired-state file without connecting to Kafka.

### 5. Generate and review a plan

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete plan -o plan.json
```

**`plan` is read-only.** `--skip-acls` limits this example to topics, and
`--no-delete` forbids deletions and RF decreases. Review the proposed changes
and require a successful exit code before continuing. A blocked plan preserves
any older output file; do not use that file as the result of a failed run.

### 6. Apply the reviewed plan

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete apply -p plan.json
```

**`apply` explicitly changes Kafka.** It rechecks RF safety against fresh
metadata. A failure can leave partial changes; there is no automatic rollback.
For command options, use `java -jar kafka-gitops-all.jar plan --help` or
`java -jar kafka-gitops-all.jar apply --help`.

## Replication Management

RF drift is included in `plan` by default. For the same metadata, target RF and
rack policy, planning produces deterministic ordered replica assignments.

| Option | Behavior |
|---|---|
| `plan --no-manage-replication` | Exclude RF changes from the plan. |
| `apply --no-manage-replication` | Reject a saved plan containing RF operations before any writes. |
| `apply --allow-replication-decrease` | Permit an intended RF decrease only after fresh safety checks. Permission is not inherited from `plan` or saved JSON. |
| Global `--no-delete` | Forbid RF decreases even when decrease permission is present. |

RF decreases appear in a plan without prior approval. To execute one, use the
separate apply permission above without `--no-delete`; the Quick Start retains
that guard. `--manage-replication` remains a deprecated compatibility alias.

Fresh checks cover cluster/topic identity, assignments, broker/rack metadata,
leader and full ISR, `min.insync.replicas`, and conflicting reassignments. With
ELR enabled at supported level 1, ELR and LastKnownELR must be known and empty.
Critical unknown or unsafe evidence blocks execution. Older Kafka requires
positive evidence of non-applicability when those fields are unavailable.

Completion requires exact target assignments, full target ISR and no active
reassignment; an acknowledgement alone is insufficient. See
[replication and ELR details](docs/design/elr-replication-compatibility.md).

## Downloads and Documentation

| Resource | Purpose |
|---|---|
| [Latest release: v0.4.0](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0) | Universal JAR, EN/RU release-note assets and `SHA256SUMS`. |
| [English Release Notes](docs/release/kafka-gitops-v0.4.0-release-notes-en.md) | Complete v0.4.0 features, compatibility, usage and test evidence. |
| [Документация на русском](docs/release/kafka-gitops-v0.4.0-release-notes-ru.md) | Полное описание v0.4.0 на русском языке. |
| [Installation and source build](docs/installation.md) | Java requirements, Gradle Wrapper and build instructions. |
| [CHANGELOG](CHANGELOG.md) | Change history and migration from 0.3.1. |
| [SECURITY](SECURITY.md) | Security policy, reporting guidance and dependency-risk boundaries. |
| [CONTRIBUTING](CONTRIBUTING.md) | Contribution and review guidelines. |

For final release status and live-test coverage, use the EN/RU release notes and
final acceptance report linked above. Older preparation-status statements in
other documents describe their recorded stage.

## Known Limitations

- ADS 4.3.1 has not been tested on a real cluster; vendor-specific features and
  policies remain unverified.
- The full SASL/TLS/Kerberos/OAuth integration matrix, managed Kafka/Confluent
  Cloud and ACL enforcement acceptance are incomplete.
- No production certification or complete native OS/CPU coverage is claimed.
  Large data volumes, sustained load, broker failures and recovery need more tests.
- Partition decreases and RF execution with nonempty ELR/LastKnownELR are
  unsupported. Preflight and writes are not an atomic Kafka transaction.
- Documented Gradle build-tool advisories are separate from runtime JAR
  dependencies; see the [security policy](SECURITY.md).

## Project Origin and License

This is an independent community-maintained continuation of
[joschi/kafka-gitops 0.3.1](https://github.com/joschi/kafka-gitops).
The original project is [devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops),
created by **Shawn Seymour**. See [provenance and modifications](CHANGES-FROM-UPSTREAM.md).

Copyright (c) 2020 Shawn Seymour. Licensed under the [Apache License 2.0](LICENSE).
Bundled components retain their own licenses and attribution; see the
[third-party notices](legal/THIRD-PARTY-NOTICES).

This fork is not endorsed or officially supported by the Apache Software
Foundation, Arenadata, Confluent, or the original maintainers.
