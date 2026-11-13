<!-- SPDX-License-Identifier: Apache-2.0 -->
# Security policy and limitations

This is an independent community-maintained fork. No vendor security support,
response SLA or production certification is claimed. Version 0.4.0 is being
prepared and has not been published in a new GitHub fork yet.

## Reporting a vulnerability

No private contact or advisory URL has been designated for the future fork.
Until the owner publishes a verified private reporting channel, do not post
credentials, private infrastructure details or an exploitable proof of concept
in a public issue. Use an already established private channel to the project
owner, if one exists, and provide only a sanitized description initially.
The owner must publish the reporting destination before accepting private reports.
Do not infer a contact address from upstream authors or their Git history.

An exposed working credential must be revoked/rotated. Removing it from the
current file or redacting a report does not remove it from history, releases
or logs. History changes require a separate decision; automated cleanup is not provided.

## Scope of verification

- Java 21 and one Kafka Client 4.4.0 universal JAR.
- Isolated live evidence exists for the preceding artifact against Apache Kafka
  3.2.3, 3.7.1 and 4.4.0. Release 0.4.0 packaging has separate unit/CLI/artifact
  evidence; old live results must not be attributed to its new hash.
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

Gradle 9.9.1 bundles Guava 33.4.6-jre and Jackson Databind 2.22.3 affected by
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
