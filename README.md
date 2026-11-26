<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Kafka GitOps

Declarative management of Apache Kafka topics, configurations and ACLs.

[![Release: v0.4.1](https://img.shields.io/badge/release-v0.4.1-2563eb?style=flat-square)](https://github.com/Hellfire-son/kafka-gitops/releases)
[![Java 21](https://img.shields.io/badge/Java-21-ea580c?style=flat-square)](docs/installation.md)
[![Apache License 2.0](https://img.shields.io/badge/license-Apache%202.0-475569?style=flat-square)](LICENSE)

**[English documentation](docs/release/kafka-gitops-v0.4.1-release-notes-en.md)** ·
**[Документация на русском](docs/release/kafka-gitops-v0.4.1-release-notes-ru.md)**

An independent community-maintained fork, distributed as one universal Java 21 JAR.

## Overview

Describe your Kafka topics and services in YAML and keep that desired state in
version control. Kafka GitOps compares it with the cluster through standard Kafka
Admin APIs and produces a plan of the required topic, configuration and ACL changes.

**Describe → Plan → Review → Apply.** Planning only reads cluster state. Changes
run through an explicit `apply`, with fresh safety checks for replication
operations. Service definitions generate ACLs using the existing profiles;
ACL/DESCRIBE semantics are unchanged.

## What's New in v0.4.1

PLAN shows detailed blocks for changes, warnings and blockers. PLAN and APPLY
share action labels and a SUMMARY. With `--verbose`, existing INFO and
`NO_CHANGE` messages remain visible. APPLY distinguishes confirmed changes,
no-ops, blocked operations, partial completion and unknown results. The
release fixes QA-001 through QA-004. Dependency versions are unchanged
from v0.4.0.

## Compatibility

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

## Quick Start

Use an existing local Kafka cluster for this example. It manages a topic, skips
ACLs and disables deletion. Java 21 is required.

### 1. Download the JAR

Download `kafka-gitops-all.jar` from the v0.4.1 entry under
[GitHub Releases](https://github.com/Hellfire-son/kafka-gitops/releases).
Compare its SHA-256 with the `SHA256SUMS` asset, then check the version:

```bash
sha256sum kafka-gitops-all.jar
java -jar kafka-gitops-all.jar --version
```

The version should be `0.4.1`. In PowerShell, use
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

In v0.4.1, detailed blocks label proposed topic and ACL changes
as `CREATE`, `UPDATE` or `DELETE`. The summary counts unchanged and blocked
topics independently and shows skipped categories. See the
[v0.4.1 release notes](docs/release/kafka-gitops-v0.4.1-release-notes-en.md).

### 6. Apply the reviewed plan

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete apply -p plan.json
```

**`apply` explicitly changes Kafka.** It rechecks RF safety against fresh
metadata. A failure can leave partial changes; there is no automatic rollback.
The v0.4.1 apply report uses the same action labels and summarizes confirmed,
partial, blocked and unknown outcomes. A saved JSON plan has no verified total
for unchanged topics, so the report displays `Unchanged topics: UNKNOWN`.
A saved ACL deletion reports `NO_OP` with exit code 0 when Kafka confirms that
the binding is already absent. If partition drift stops a write during the
immediate safety check, APPLY reports `BLOCKED` and exits with code 2.
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
| [Release: v0.4.1](https://github.com/Hellfire-son/kafka-gitops/releases) | Universal JAR, EN/RU release-note assets and `SHA256SUMS`. |
| [English Release Notes](docs/release/kafka-gitops-v0.4.1-release-notes-en.md) | Complete v0.4.1 features, compatibility, usage and test evidence. |
| [Документация на русском](docs/release/kafka-gitops-v0.4.1-release-notes-ru.md) | Полное описание v0.4.1 на русском языке. |
| [Installation and source build](docs/installation.md) | Java requirements, Gradle Wrapper and build instructions. |
| [CHANGELOG](CHANGELOG.md) | Change history and migration from 0.3.1. |
| [SECURITY](SECURITY.md) | Security policy, reporting guidance and dependency-risk boundaries. |
| [CONTRIBUTING](CONTRIBUTING.md) | Contribution and review guidelines. |

For v0.4.0, see the [historical release notes](docs/release/kafka-gitops-v0.4.0-release-notes-en.md).

## Known Limitations

- ADS 4.3.1 has not been tested on a real cluster; vendor-specific features and
  policies remain unverified.
- Managed Kafka and Confluent Cloud remain outside the live test matrix.
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
