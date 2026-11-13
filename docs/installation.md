<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Modified for the independent 0.4.0 fork: Java 21, universal JAR and source build instructions. -->
# Installation and build

Kafka GitOps 0.4.0 is an independent community-maintained fork. The future
fork/release URL has not been assigned; use the download page designated by
the owner after publication. Old devshawn Homebrew and joschi container/release
packages do not distribute this new binary.

## Requirements

- Java 21 to run the JAR; JDK 21 to build and run the local tests.
- Configure Java through `JAVA_HOME` or `PATH`; environment variables for Kafka
  are optional when `--command-config` supplies all required properties.
- Gradle Wrapper 9.9.1 and Shadow 9.6.1 are part of the source build.
- A Kafka connection is required for plan/apply, not for help/version/local validation.

## Universal JAR

Download the single `kafka-gitops-all.jar` from the confirmed fork's release
page. Compare its SHA-256 to the checksum published for that exact release.
Do not use the old compatibility-test hash to verify a newly packaged binary.

```bash
sha256sum kafka-gitops-all.jar
java -jar kafka-gitops-all.jar --version
java -jar kafka-gitops-all.jar --help
```

PowerShell:

```powershell
Get-FileHash .\kafka-gitops-all.jar -Algorithm SHA256
java -jar .\kafka-gitops-all.jar --version
```

The version must be `0.4.0`. Legal texts are under `META-INF/licenses/` and
`META-INF/THIRD-PARTY-NOTICES` inside the JAR. The optional
`kafka-gitops-0.4.0.zip` contains a POSIX launcher plus readable legal materials.
Extract it, preserve the supplied notices, and invoke `./kafka-gitops --help`
on an accepted POSIX environment. Windows users invoke the JAR with Java.
No new Docker image or Homebrew package is published by this preparation.

## Build from source

From the repository root, with JDK 21 selected:

```bash
./gradlew --no-daemon --no-scan clean classes testClasses test shadowJar verifyReleaseArtifact buildExecutableJar buildRelease
python3 -B -m unittest discover -s tools/integration -p 'test_*.py'
python3 -B tools/release/verify_release.py
```

On Windows use `.\gradlew.bat` and `python` for those same tasks.
The first Gradle invocation downloads the pinned wrapper/dependencies; use
`--offline` only after the required artifacts are cached. No installed Gradle
is required. The `test` task excludes external-cluster `*IntegrationSpec`
suites and never starts Docker/Kafka. Do not run legacy integration suites
against a cluster without separate acceptance authorization.

Outputs:

- `build/libs/kafka-gitops-all.jar` — the only runtime JAR.
- `build/output/kafka-gitops` — POSIX launcher with the same embedded JAR.
- `build/distributions/kafka-gitops-0.4.0.zip` — launcher, notes and legal bundle.

`verifyReleaseArtifact` validates licenses, dependency bytes, Java 21 bytecode,
Kafka Client 4.4.0, services and JNI resources. `buildExecutableJar` requires
that verification. Archives have stable order/timestamps and contain no build
host, user or build-time stamp. Check the final hash after every packaging change.

## Configure and plan

Keep `client.properties` outside Git. For a disposable local PLAINTEXT lab only:

```properties
bootstrap.servers=127.0.0.1:9092
security.protocol=PLAINTEXT
```

```bash
java -jar kafka-gitops-all.jar --command-config client.properties -f desired.yaml validate
java -jar kafka-gitops-all.jar --command-config client.properties -f desired.yaml plan -o plan.json
# Review first; this is the separate write step:
java -jar kafka-gitops-all.jar --command-config client.properties apply -p plan.json
```

Explicit file values override environment values. There is no implicit bootstrap
fallback. For authenticated clusters supply the appropriate Kafka properties
and keep credentials outside source, logs and release assets. The local example
is not a production security configuration.

RF planning is enabled by default. `--no-manage-replication` opts out.
A decrease requires `apply --allow-replication-decrease`; `--no-delete` forbids
it. See [quick start](quick-start.md), [release notes](release/v0.4.0-release-notes.md)
and [SECURITY.md](../SECURITY.md) for safety/compatibility limits.
