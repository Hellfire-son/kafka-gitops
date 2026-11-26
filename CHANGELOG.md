<!-- SPDX-License-Identifier: Apache-2.0 -->
# Changelog

## 0.4.1

- Plan and apply use the same `CREATE`, `UPDATE` and `DELETE` labels, resource
  details and config transitions. Detailed blocks omit unchanged topics without
  diagnostics. Existing INFO and `NO_CHANGE` messages remain available with
  `--verbose`.
- Plan summaries count distinct blocked topics separately from blocking
  diagnostics and unchanged topics. Both reports mark skipped resource types.
- Apply summaries count recorded outcomes, including partial changes, rejected
  writes, unknown results and operations that did not run. RF completion is
  shown after its existing verification. The total of unchanged topics is
  unknown for a saved JSON plan.
- **QA-001.** ACL deletion counts bindings confirmed by Kafka. Reapplying a saved deletion
  plan after the binding is gone reports `NO_OP` and exits with code 0.
- **QA-002.** Partition drift detected immediately before a write reports `BLOCKED`.
  An unconfirmed submitted write reports `UNKNOWN`. Later operations stay
  `NOT_RUN`, and a run with prior confirmed changes reports `APPLY_PARTIAL`.
- **QA-004.** Final verification of an RF decrease can wait within its existing deadline
  when the assignment matches the ordered target and only known removed replicas
  remain alongside the full target ISR. Success requires the exact target ISR
  on every partition and fresh safety checks. The reads and waits share one
  monotonic deadline, including when a thread resumes after a delay.
- CLI options, exit codes, executable JSON format and apply behavior are
  preserved. Read-only planning, sensitive-value redaction, RF safety checks,
  fresh preflight and reassignment completion checks continue to apply.
- **QA-003.** The external integration tests use per-case output paths, compare normalized
  transcripts and check Kafka state after PLAN and APPLY. Round 3 passed 1,153 JVM
  tests and external IntegrationSpec on Java 21 (76/76 Windows, 76/76 Linux).
- Local live tests passed on Kafka 3.2.3, 3.7.1, 4.4.0 and cp-kafka 7.9.11,
  including RF 4 → 6 across three racks on Confluent.
- Java 21, Kafka Client 4.4.0, Gradle 9.9.1 and Shadow 9.6.1 are unchanged.

[English Release Notes](docs/release/kafka-gitops-v0.4.1-release-notes-en.md) ·
[Описание выпуска на русском](docs/release/kafka-gitops-v0.4.1-release-notes-ru.md)

## [0.4.0](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0) — 2026-11-15

Independent continuation of joschi/kafka-gitops 0.3.1, originally created by
Shawn Seymour as devshawn/kafka-gitops. The original Apache-2.0 license,
copyright and `com.devshawn.*` packages are retained.

- Java 21; Kafka Client 4.4.0; Gradle Wrapper 9.9.1; Shadow 9.6.1.
- Updated Jackson 3.3.0, Logback 1.6.6, SLF4J 2.0.20, Picocli 4.7.7,
  SnakeYAML Engine 3.2, LZ4 1.14.0, Snappy Java 1.1.10.12 and Zstd JNI 1.5.7-26.
- Topic creation and config management, safe partition increases, deterministic
  replication planning, fresh apply preflight and completion checks.
- RF increase/decrease and ELR/LastKnownELR safety evidence handling.
- `--command-config` with file-over-environment precedence and explicit bootstrap;
  local validation needs no Kafka connection. Sensitive diagnostic values are redacted.
- Text plan diagnostics are separated from executable JSON; blocked plans are
  not exported as executable JSON. Saved RF plans are rechecked on apply.
- One universal JAR with preserved component-specific Apache/MIT/EPL/BSD/native
  legal texts, source availability, service descriptors and JNI resources.
- Safe build-only CI; no automatic release, container, Pages, Sonar or build-scan
  publication. Portable private-file exclusions and anonymized current docs.

### Migration from 0.3.1

RF drift is included in `plan` by default. Use `--no-manage-replication` to
retain automation that excludes RF operations. `--manage-replication` is a
deprecated compatibility alias. RF decreases are shown by `plan` but require
an independent `apply --allow-replication-decrease`; `--no-delete` forbids them.
Planning does not apply changes. Partition decreases remain unsupported.

### Evidence and limitations

Earlier isolated integration trials covered topics, configs, partition increases
and RF 1→2→3→2 on Apache Kafka 3.2.3, 3.7.1 and 4.4.0 using the preceding artifact.
The final release JAR then passed separate live smoke tests on 2026-11-14:

- Kafka 3.2.3: plan, test topic creation and RF 1→2.
- Kafka 3.7.1: plan, test topic creation and RF 1→2.
- Kafka 4.4.0: plan, test topic creation and RF 1→2→3→2 with ELR enabled at level 1.

The final Kafka 3.x smoke tests do not repeat the whole earlier integration matrix.
Final verification also recorded 1039 Java tests, 26 Python guard tests,
56 CLI smoke checks and 156 codec/JNI checks — PASS, separately from live trials.
ADS 4.3.1 is NOT TESTED LIVE; full SASL/TLS/Kerberos/OAuth and ACL enforcement
acceptance remain incomplete. Production certification is not claimed.
Known Guava/Jackson findings inside Gradle are separate from runtime JAR
dependencies. Old Kafka brokers require their own security review.

See [English Release Notes](docs/release/kafka-gitops-v0.4.0-release-notes-en.md),
[описание выпуска на русском](docs/release/kafka-gitops-v0.4.0-release-notes-ru.md),
[final acceptance](docs/release/v0.4.0-final-review.md),
[historical preparation evidence](docs/release/v0.4.0-release-preparation.md)
and [security policy](SECURITY.md).

## 0.3.1 — upstream base

The preserved upstream base is joschi/kafka-gitops tag `0.3.1`, commit
`a16eb39c45e609d19ca2b12ff6def3e22a46dffc`. Earlier tags and authorship belong
to upstream history; no new release dates or upstream release notes are invented here.
