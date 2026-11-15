<!-- SPDX-License-Identifier: Apache-2.0 -->
# Security policy and limitations

This is the independent community-maintained
[Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops) fork.
[Version 0.4.0 is published](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0).
No vendor security support, response SLA or production certification is claimed.

## Reporting a vulnerability

Use GitHub's private
[Report a vulnerability form](https://github.com/Hellfire-son/kafka-gitops/security/advisories/new).
GitHub sign-in is required. Private Vulnerability Reporting was confirmed enabled
on 2026-11-15 through the repository API and the Report a vulnerability button on
its [Security page](https://github.com/Hellfire-son/kafka-gitops/security).

Include the affected version, a sanitized reproduction, impact and relevant
configuration without working credentials. Do not use public Issues or PRs for
secrets, private infrastructure details or undisclosed vulnerability details.
If the private form becomes unavailable, the owner must verify the setting and
publish a confirmed private alternative before sensitive reports are sent.
No personal email address or upstream security contact is designated here.

An exposed working credential must be revoked/rotated. Removing it from the
current file or redacting a report does not remove it from history, releases
or logs. History changes require a separate decision; automated cleanup is not provided.

## Scope of verification

- Java 21 and one Kafka Client 4.4.0 universal JAR.
- Earlier integration tests on the preceding artifact covered topics, configs,
  partition increases and RF 1→2→3→2 on Kafka 3.2.3, 3.7.1 and 4.4.0.
- The final v0.4.0 JAR passed separate live smoke tests on 2026-11-14:
  Kafka 3.2.3/3.7.1 — plan, new topic and RF 1→2; Kafka 4.4.0 — plan, new topic
  and RF 1→2→3→2 with ELR enabled at level 1. This does not repeat the entire
  earlier matrix. See [final technical acceptance](docs/release/v0.4.0-final-review.md)
  and the [English](docs/release/kafka-gitops-v0.4.0-release-notes-en.md) /
  [Russian](docs/release/kafka-gitops-v0.4.0-release-notes-ru.md) release notes.
- ADS 4.3.1 is NOT TESTED LIVE. Full SASL/TLS/Kerberos/OAuth/Confluent Cloud
  and ACL enforcement acceptance is not complete. Existing ACL/DESCRIBE
  semantics are retained.
- Planning is read-only. Apply can create/change/delete Kafka resources subject
  to the existing options, permissions and fresh safety checks. RF decreases
  require `--allow-replication-decrease`; `--no-delete` forbids them.
- Use a dedicated non-production environment for acceptance. Lab scripts are
  opt-in, identity-gated and never part of automatic Java CI cluster operations.

## Credentials and plans

Keep real client properties, JAAS, keytabs, certificates/private keys and tokens
outside Git and release assets. `--command-config` can supply all required
Kafka properties; no environment variables are mandatory. Explicit file values
override corresponding environment values; configure bootstrap servers explicitly.
Do not put credentials on the command line or use demo passwords in real accounts.
Plans and diagnostics can contain resource names and principals even when
credential values are redacted; review before sharing them.

## Known dependency risks

The release assessment dated 2026-11-13 records that Gradle 9.9.1 bundles
Guava 33.4.6-jre and Jackson Databind 2.22.3 affected by
[CVE-2026-102554](https://github.com/google/guava/security/advisories/GHSA-xxph-c9ww-hj94)
and [CVE-2026-110879](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-m8xf-2cr7-77ph).
These are build-environment findings; those versions are not runtime components
of the universal JAR. Concrete exploitation through this build has not been
demonstrated. Use isolated builds with trusted inputs and no publishing secrets;
do not replace Gradle internal JARs with unsupported overrides.

The dated SCA evidence is not a guarantee of current vulnerability absence.
Runtime, build tools, test dependencies, native binaries and container/JRE images
have different scopes. Old broker vulnerabilities remain even with a newer client.
See [release preparation](docs/release/v0.4.0-release-preparation.md) for evidence,
remaining warnings and the date of advisory checks. No automatic external build
scans, Sonar uploads or package/container/Pages publishing are enabled in CI.
