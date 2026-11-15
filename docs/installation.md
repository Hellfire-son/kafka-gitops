<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Installation and build

Kafka GitOps **0.4.0** is published in the independent
[Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops) repository.
Use the [v0.4.0 release](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0)
for downloads and the [English](release/kafka-gitops-v0.4.0-release-notes-en.md) /
[Russian](release/kafka-gitops-v0.4.0-release-notes-ru.md) notes for tested behavior.

## Requirements

- **Java 21** to run the universal JAR; **JDK 21** for source builds and tests.
- Select Java through `JAVA_HOME` or `PATH`.
- The JAR includes Kafka Client **4.4.0**; no separate Kafka client installation
  or version-specific GitOps build is required.
- A connection to Kafka is needed for plan/apply, not for help, version or local
  YAML validation. No Kafka environment variables are mandatory when
  `--command-config` provides all required settings.

## Published release files

The v0.4.0 GitHub Release contains these four uploaded assets, in addition to
GitHub's generated source archives:

| File | Purpose |
|---|---|
| [kafka-gitops-all.jar](https://github.com/Hellfire-son/kafka-gitops/releases/download/v0.4.0/kafka-gitops-all.jar) | The universal runtime JAR. |
| [English release notes](https://github.com/Hellfire-son/kafka-gitops/releases/download/v0.4.0/kafka-gitops-v0.4.0-release-notes-en.md) | Complete English release documentation. |
| [Русские Release Notes](https://github.com/Hellfire-son/kafka-gitops/releases/download/v0.4.0/kafka-gitops-v0.4.0-release-notes-ru.md) | Полное описание выпуска на русском языке. |
| [SHA256SUMS](https://github.com/Hellfire-son/kafka-gitops/releases/download/v0.4.0/SHA256SUMS) | SHA-256 for the JAR and both Markdown assets. |

The JAR is **26 018 771 bytes**. Its verified SHA-256 is:

```text
a11deccb969864be1cd30f93cddc4cbbe4f2f395a1f3cc6619400f25f4524f4b
```

### Linux and macOS

Download the JAR and compare its checksum with the value above or `SHA256SUMS`:

```bash
sha256sum kafka-gitops-all.jar
```

On macOS, use:

```bash
shasum -a 256 kafka-gitops-all.jar
```

Then run with Java 21:

```bash
java -jar kafka-gitops-all.jar --version
java -jar kafka-gitops-all.jar --help
```

The version must be `0.4.0`. If all three files named in `SHA256SUMS` were
also downloaded, `sha256sum -c SHA256SUMS` verifies the complete set on Linux.

### Windows PowerShell

```powershell
Get-FileHash .\kafka-gitops-all.jar -Algorithm SHA256
java -jar .\kafka-gitops-all.jar --version
java -jar .\kafka-gitops-all.jar --help
```

Compare the reported hash with the expected value above before use.

## Configure and run

Keep `client.properties` outside public Git. For a disposable local PLAINTEXT lab:

```properties
bootstrap.servers=127.0.0.1:9092
security.protocol=PLAINTEXT
```

For authenticated clusters, supply the appropriate security, authentication and
trust properties privately. Explicit file values override corresponding
`KAFKA_*` environment values; environment can fill missing properties. There is
no implicit bootstrap fallback. An unreadable or malformed file fails the command.

With a prepared `state.yaml`, this topic-only example disables deletion:

```bash
java -jar kafka-gitops-all.jar -f state.yaml validate
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete -f state.yaml plan -o plan.json
```

Review the changes and check the exit code. **Planning is read-only.** Apply is
an explicit, separate write step:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --skip-acls --no-delete apply -p plan.json
```

See [Quick Start](quick-start.md) for the complete example, [permissions](permissions.md)
for authorization and [SECURITY.md](../SECURITY.md) for credential handling and
acceptance limits. RF planning is enabled by default. `--no-manage-replication`
opts out; a decrease requires independent `apply --allow-replication-decrease`
without `--no-delete`, plus fresh safety checks.

## Build from source

Use JDK 21 and the checked-in Gradle Wrapper **9.9.1** with Shadow **9.6.1**.
No installed Gradle is required. Tag `v0.4.0` identifies the original release
source; `main` can contain later documentation changes. From the repository root:

```bash
./gradlew --no-daemon --no-scan clean classes testClasses test shadowJar verifyReleaseArtifact buildExecutableJar buildRelease
python3 -B -m unittest discover -s tools/integration -p 'test_*.py'
python3 -B tools/release/verify_release.py
```

Windows PowerShell:

```powershell
.\gradlew.bat --no-daemon --no-scan clean classes testClasses test shadowJar verifyReleaseArtifact buildExecutableJar buildRelease
python -B -m unittest discover -s tools/integration -p 'test_*.py'
python -B tools/release/verify_release.py
```

The first build downloads the pinned wrapper and dependencies. Use `--offline`
only when the required artifacts are cached. Java unit tests exclude external
`*IntegrationSpec` suites; the Python guards and release verifier are offline
checks. These commands do not start Docker/Kafka or perform cluster writes.
External-cluster acceptance runners require separate authorization.

## Locally built artifacts

| Local output | Contents |
|---|---|
| `build/libs/kafka-gitops-all.jar` | The only runtime JAR. |
| `build/output/kafka-gitops` | POSIX launcher with the same JAR embedded. |
| `build/distributions/kafka-gitops-0.4.0.zip` | POSIX launcher, notes and legal bundle. |

The Gradle-generated ZIP is a **local build output**, not an uploaded binary
asset of the public v0.4.0 release. GitHub's generated source ZIP is a separate
source archive. A locally built POSIX launcher can be run with `./kafka-gitops`;
Windows users can run the JAR directly.

`verifyReleaseArtifact` checks licenses, reviewed dependency bytes, Java 21
bytecode, Kafka Client 4.4.0, services and JNI resources. `buildExecutableJar`
requires that verification. The JAR includes `META-INF/licenses/`,
`META-INF/THIRD-PARTY-NOTICES` and `META-INF/NOTICE`. Preserve those notices when
redistributing it. Source or packaging changes require checksums for the resulting
build; do not assume a new build matches the published artifact.

Upstream Homebrew packages and Docker images are not distributions of this
independent v0.4.0 JAR.
