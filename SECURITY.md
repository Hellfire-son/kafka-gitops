<!-- SPDX-License-Identifier: Apache-2.0 -->
# Security policy and limitations

This is the independent community-maintained
[Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops) fork.
See [GitHub Releases](https://github.com/Hellfire-son/kafka-gitops/releases) for release assets.
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

Independent Final QA Round 3 exercised the v0.4.1 Java 21 JAR on Kafka
3.2.3, 3.7.1, 4.4.0 with ELR level 1, and Confluent cp-kafka 7.9.11.
The Confluent lab verified SASL/PLAIN, ACL enforcement and repeated ACL
deletion. ADS, Kerberos, TLS and OAuth were outside the live test matrix.

Planning is read-only. Apply uses the existing options, permissions and fresh
safety checks. RF decreases require `--allow-replication-decrease` and
`--no-delete` forbids them. Round 3 verified redaction using synthetic
credentials. Gitleaks reported zero findings in the scanned release artifacts.
Dependency versions are unchanged from v0.4.0. See the
[English](docs/release/kafka-gitops-v0.4.1-release-notes-en.md) and [Russian](docs/release/kafka-gitops-v0.4.1-release-notes-ru.md) notes.

Use a dedicated non-production environment for acceptance. Lab scripts are
opt-in and check the fixture identity before changing Kafka resources.

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
