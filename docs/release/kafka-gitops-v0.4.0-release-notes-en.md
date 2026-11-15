<!-- SPDX-License-Identifier: Apache-2.0 -->
# Kafka GitOps v0.4.0

Kafka GitOps manages Apache Kafka topics, configurations and ACLs from a
version-controlled desired state. Version 0.4.0 modernizes the Java runtime and
dependencies, improves plan/apply diagnostics, and adds partition increases and
replication-factor management with fresh safety checks and ELR-aware execution.

## About

This is an independent community-maintained fork at
[Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops), based on
[joschi/kafka-gitops 0.3.1](https://github.com/joschi/kafka-gitops). The original
project is [devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops), created
by **Shawn Seymour**. The original copyright and `com.devshawn.*` package names
are retained.

The project uses the
[Apache License 2.0](https://github.com/Hellfire-son/kafka-gitops/blob/main/LICENSE).
Bundled third-party components retain their respective licenses and notices.
This fork is not endorsed or officially supported by the Apache Software
Foundation, Arenadata, Confluent, or the original maintainers.

## What's new

Changes since the upstream 0.3.1 base:

- **Runtime and build:** Java 21, Kafka Client 4.4.0, Gradle Wrapper 9.9.1 and
  Shadow 9.6.1. One universal JAR serves the tested Kafka 3.x and 4.x versions.
- **Dependencies:** Jackson Core/Databind/YAML 3.3.0 with Annotations 2.23,
  Logback 1.6.6, SLF4J 2.0.20, Picocli 4.7.7, SnakeYAML Engine 3.2,
  LZ4 Java 1.14.0, Snappy Java 1.1.10.12 and Zstd JNI 1.5.7-26.
- **Configuration:** `--command-config` accepts standard Kafka client properties.
  Explicit file values override corresponding environment values; environment
  values can supply properties missing from the file. Kafka environment
  variables are optional. Bootstrap servers must be configured explicitly.
- **Diagnostics:** bounded connection/metadata waits, stable error categories,
  and distinct configuration, connection, metadata and planning stages.
  Sensitive configuration and sensitive old/new topic values are redacted.
- **Readable plans:** actual and desired partition/RF values, old and new config
  values, ordered replica assignments, broker/rack facts, warnings and blockers.
  Text diagnostics are separate from executable JSON. Blocked plans are not
  exported; an existing output file remains unchanged.
- **Topic operations:** topic creation and configuration changes, partition
  increases, and deterministic RF increases/decreases. Partition decreases
  remain unsupported. Existing ACL profiles and opt-in DESCRIBE semantics are
  preserved; automatic DESCRIBE grants are not introduced.
- **ELR and older brokers:** capability evidence distinguishes unsupported or
  unknown metadata from positively established non-applicability. ELR-enabled
  execution supports verified empty ELR/LastKnownELR sets and healthy ISR.
- **Apply:** fresh permission and metadata checks before RF writes, repeated
  checks between batches, bounded reassignment monitoring and independent
  completion verification. A request acknowledgement alone is not success.

## Replication factor management

RF drift is included in `plan` by default. `--no-manage-replication` excludes RF
changes; on `apply` it rejects any saved plan containing an RF operation before
all writes. `--manage-replication` remains a deprecated compatibility alias.
Opposite RF flags conflict.

**Increase:** deterministic placement selects verified eligible brokers while
preserving the ordered assignment context. The default
`plan --replication-rack-policy RACK_AWARE` requires complete rack information
and checks rack diversity. `BROKER_ONLY` must be selected explicitly and accepts
the loss of rack protection; it does not bypass the other safety checks.

**Decrease:** `plan` shows decreases without requiring approval. Execution
requires a fresh `apply --allow-replication-decrease`; the compatibility option
on `plan` does not grant that permission. Global `--no-delete` forbids an RF
decrease even when `--allow-replication-decrease` is present. The target must
retain the current leader, use replicas from the original ISR, satisfy the rack
policy and remain at or above effective `min.insync.replicas`. GitOps does not
automatically lower `min.insync.replicas`.

Fresh checks cover cluster ID, topic UUID, partition count/IDs, previous ordered
assignments, broker topology/racks, reachability, fencing, cordoning, log-directory
health, effective configs, feature evidence, snapshot freshness and conflicting
reassignments. Saved JSON is neither current metadata nor permission to write.
Identity, topology, config or assignment drift can invalidate a plan and require
replanning. Incompatible simultaneous partition/config changes are rejected.

ISR must be healthy with a valid leader. ELR membership does not prove current
synchronization. With ELR enabled at supported level 1, both ELR and LastKnownELR
must be known and empty. Nonempty or invalid sets block RF execution. Missing
fields remain `UNKNOWN`, never fabricated empty lists. Older Kafka can proceed
when separate positive evidence establishes ELR `NOT_APPLICABLE`; absence of a
field or a feature alone is insufficient.

Critical `UNKNOWN` evidence, unsafe ISR/ELR, insufficient eligible brokers/racks,
stale identity/assignments, conflicting reassignment, insufficient target RF/minISR
or missing permission produces `BLOCKED`. Specific noncritical uncertainty about
generic placement admission or disk reserve can remain a warning; this does not
prove sufficient capacity or waive critical checks. A successful preview still
requires `FRESH_PREFLIGHT_REQUIRED` on apply.

The executor distinguishes acknowledgement/`ACCEPTED`, `RUNNING` and `COMPLETED`.
Completion requires exact ordered target assignments, full target ISR, a valid
leader, stable identity and safety evidence, and no active reassignment.
Independent final reads can wait for metadata-cache convergence within the
deadline. Missing terminal proof yields failure/`UNKNOWN`, not success. There is
no automatic rollback, cancellation or substitution of target assignments.

**`plan` never changes Kafka. Changes run only through an explicit `apply`.**

## Compatibility

Historical integration trials and the final release-JAR acceptance are separate
evidence sets. The final JAR is the artifact identified by the SHA-256 below.

| Target | Earlier integration trials | Final v0.4.0 JAR acceptance, 2026-11-14 |
|---|---|---|
| Apache Kafka 3.2.3 | PASS: topics, configs, partition increases, RF 1 → 2 → 3 → 2 | PASS: plan, new test topic, RF 1 → 2 |
| Apache Kafka 3.7.1 | PASS: topics, configs, partition increases, RF 1 → 2 → 3 → 2 | PASS: plan, new test topic, RF 1 → 2 |
| Apache Kafka 4.4.0 | PASS: topics, configs, partition increases, RF 1 → 2 → 3 → 2; separate ELR trials | PASS: plan, new test topic, RF 1 → 2 → 3 → 2; ELR remained enabled at level 1 |
| Arenadata Streaming / ADS 4.3.1 | **NOT TESTED LIVE** | **NOT TESTED LIVE**; no verified vendor compatibility claim |

The Kafka 3.x final checks do not establish that the entire earlier matrix was
rerun on the release JAR. Results cover the recorded isolated scenarios, not all
cluster configurations. No vendor certification or production certification is
claimed. Kafka 3.x compatibility is not a recommendation for new production
deployments; a newer client does not patch old broker vulnerabilities.

See the
[historical compatibility results](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/testing/kafka-3x-compatibility-results.md)
and [final technical acceptance](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/release/v0.4.0-final-review.md).

## Installation

Use **Java 21** and the single **`kafka-gitops-all.jar`**. Verify its SHA-256
against the Artifacts section before running it:

```bash
sha256sum kafka-gitops-all.jar
java -jar kafka-gitops-all.jar --version
java -jar kafka-gitops-all.jar --help
```

PowerShell checksum verification:

```powershell
Get-FileHash .\kafka-gitops-all.jar -Algorithm SHA256
```

The CLI version must be `0.4.0`. Prepare `desired.yaml` and a private
`client.properties` with explicit `bootstrap.servers`, the required security
protocol and any authentication/trust settings for your cluster. Keep credentials
outside Git. No `KAFKA_*` environment variables are mandatory when the file
supplies all required settings. Unreadable or malformed properties fail without
falling back silently.

Validate locally, then generate and review a plan:

```bash
java -jar kafka-gitops-all.jar -f desired.yaml validate
java -jar kafka-gitops-all.jar --command-config client.properties -f desired.yaml plan -o plan.json
```

Validation needs no Kafka connection; plan/apply do. Inspect all topic and ACL
operations and check the exit code before the separate write step:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties apply -p plan.json
```

Only for a reviewed, intended RF decrease:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties apply -p plan.json --allow-replication-decrease
```

For automation that must exclude RF, use `plan --no-manage-replication`.
`plan --help` and `apply --help` describe the command-specific options. Review
fresh preflight results even when applying a previously saved JSON plan.

## Security

Sensitive client properties and sensitive old/new plan values are masked.
Diagnostics use bounded waits and categorized errors rather than raw Kafka
exception payloads. Plans and logs may still contain resource names, principals
and permitted connection metadata; review them before sharing.

RF safety fails closed when required facts or permissions are missing. Decrease
approval, `--no-delete`, fresh metadata, ISR/ELR and minISR checks remain
independent requirements. Planning never triggers automatic application. A blocked
plan does not replace an existing JSON file: do not apply that older file as the
output of a failed planning run.

Preflight and reassignment are not an atomic Kafka transaction. State can change
between checks and writes, and partial changes can remain after a failure. JSON
schema v1 does not pin the original leader. Generic placement admission and the
space required for copying are not fully proven by read-only Admin metadata.
There is no automatic recovery or rollback. These checks do not replace an
operator's security, capacity and recovery review.

## Testing

Final recorded acceptance results:

| Verification layer | Result | Scope |
|---|---|---|
| Java regression tests | **1039 PASS** | 48 suites; failures/errors/skipped = 0; external-cluster integration suites excluded |
| Python guard tests | **26 PASS** | Offline integration-runner safety guards |
| Actual JAR CLI smoke checks | **56 PASS** | Version/help, config/validation, legacy JSON and option guards |
| Standalone codec/JNI checks | **156 PASS** | LZ4/Snappy/Zstd arrays, direct buffers and Kafka batches; Windows x64 / Java 21 |
| Final JAR live checks | **PASS** | Kafka 3.2.3, 3.7.1 and 4.4.0, limited to the Compatibility table |

The live checks independently confirmed target assignments, full ISR, a valid
leader, preserved topic UUID and no active reassignment. Repeated plan/apply
checks reported no changes/zero writes after completion. Each new test topic
preserved 12/12 synthetic records, including keys, values, partitions and offsets.
On Kafka 4.4.0, refusal checks covered excessive RF, stale RF JSON, decrease
without permission and decrease under `--no-delete`.

Live results are separate from unit, offline guard and CLI/JNI checks. On Kafka
4.4.0, intermediate `RUNNING` was not independently sampled during the very short
RF 3 → 2 transition; its final completion passed all terminal checks. All bundled
native OS/CPU combinations were not tested. These are recorded results from the
[final review](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/release/v0.4.0-final-review.md),
not a claim of new test runs during documentation preparation.

## Artifacts

| Property | Value |
|---|---|
| Runtime file | `kafka-gitops-all.jar` |
| Version | `0.4.0` |
| Java requirement | Java 21 |
| Kafka Client | 4.4.0 |
| Size | 26 018 771 bytes |
| SHA-256 | `a11deccb969864be1cd30f93cddc4cbbe4f2f395a1f3cc6619400f25f4524f4b` |

The release asset set consists of `kafka-gitops-all.jar`,
`kafka-gitops-v0.4.0-release-notes-en.md`,
`kafka-gitops-v0.4.0-release-notes-ru.md` and `SHA256SUMS`. The checksum file uses
the standard `<sha256>  <filename>` format for the JAR and both Markdown files.

Licenses, original attribution and third-party notices are included inside the
JAR at `META-INF/licenses/`, `META-INF/THIRD-PARTY-NOTICES` and `META-INF/NOTICE`.
The reviewed bundle covers 13 runtime components and 58 third-party legal files.
Logback uses its EPL-2.0 option; MIT/BSD/native terms remain applicable to their
components.

Tag `v0.4.0` identifies release commit `04cc7f7`. These bilingual notes are
maintained in a subsequent documentation commit on `main` and supplied separately
as release assets; they are not part of the original tagged source snapshot.
The tag and runtime artifact retain their original identity.

## Known limitations

- ADS 4.3.1 has not been tested against a real cluster; vendor-specific features,
  policies and backports are unverified.
- The full SASL/TLS/Kerberos/OAuth integration matrix, managed Kafka/Confluent
  Cloud and ACL enforcement acceptance remain incomplete. Loader/unit coverage
  is not proof of live authentication or authorization.
- Nonempty ELR/LastKnownELR and critically unknown safety evidence block RF
  execution. Partition decreases are unsupported.
- Large data volumes, sustained load, broker failures, partial reassignment and
  disaster recovery need additional tests. Production certification and full
  native-platform coverage are absent.
- Gradle build-tool advisories are documented in the
  [release preparation report](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/release/v0.4.0-release-preparation.md)
  using the dated 2026-11-13 assessment. The affected Gradle Guava/Jackson
  components are outside the runtime JAR; these findings do not establish runtime
  JAR vulnerabilities. A dated scan does not guarantee the absence of other CVEs.
- A verified private security-reporting destination is not documented. Do not
  post credentials or exploitable private details in public issues.

## Documentation

- [README](https://github.com/Hellfire-son/kafka-gitops/blob/main/README.md)
- [Changelog](https://github.com/Hellfire-son/kafka-gitops/blob/main/CHANGELOG.md)
- [Installation and source build](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/installation.md)
- [Security policy and limitations](https://github.com/Hellfire-son/kafka-gitops/blob/main/SECURITY.md)
- [Project license](https://github.com/Hellfire-son/kafka-gitops/blob/main/LICENSE)
- [Third-party licenses and source availability](https://github.com/Hellfire-son/kafka-gitops/blob/main/legal/README.md)
- [Third-party notices](https://github.com/Hellfire-son/kafka-gitops/blob/main/legal/THIRD-PARTY-NOTICES)
- [Project provenance and modifications](https://github.com/Hellfire-son/kafka-gitops/blob/main/CHANGES-FROM-UPSTREAM.md)
- [ELR and RF design](https://github.com/Hellfire-son/kafka-gitops/blob/main/docs/design/elr-replication-compatibility.md)

Earlier preparation-status statements in linked documents describe their recorded
stage. The final technical review and these notes provide the final release-JAR
hash and live-test scope.
