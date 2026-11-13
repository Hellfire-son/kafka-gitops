<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# kafka-gitops 0.4.0

Independent community-maintained fork of
[joschi/kafka-gitops 0.3.1](https://github.com/joschi/kafka-gitops),
originally created by **Shawn Seymour** as
[devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops).
Version 0.4.0 is an independent continuation. It is not endorsed or officially
supported by the Apache Software Foundation, Arenadata, Confluent, or the
original maintainers. See [provenance and modifications](CHANGES-FROM-UPSTREAM.md).

Requires **Java 21** and uses **Kafka Client 4.4.0**. One universal JAR serves
the tested Kafka versions. This release is being prepared; the new fork and
download URL have not been assigned. [Release notes](docs/release/v0.4.0-release-notes.md)
describe compatibility, migration and remaining security limitations.

Manage Apache Kafka topics and ACLs through a desired state file.

<p align="center">
    <img src="https://i.imgur.com/jnDwYp8.png"/>
</p>

## Overview

Kafka GitOps is an Apache Kafka resources-as-code tool which allows you to automate the management of your Apache Kafka topics and ACLs from version controlled code. It allows you to define topics and services through the use of a desired state file, much like Terraform and other infrastructure-as-code tools.

Topics and services get defined in a YAML file. When run, `kafka-gitops` compares your desired state to the actual state of the cluster and generates a plan to execute against the cluster. This will make your topics and ACLs match your desired state.

This tool also generates the needed ACLs for each type of application. There is no need to manually create a bunch of ACLs for Kafka Connect, Kafka Streams, etc. By defining your services, `kafka-gitops` will build the necessary ACLs.

The tool uses standard Kafka Admin APIs. The verified lab scenarios and
unverified vendor/security combinations are listed below; managed Kafka and
Confluent Cloud have not received full integration acceptance for this release.

## Features

- 🚀  **Built For CI/CD**: Made for CI/CD pipelines to automate the management of topics & ACLs.
- 🔥  **Configuration as code**: Describe your desired state and manage it from a version-controlled declarative file.
- 👍  **Easy to use**: Deep knowledge of Kafka administration or ACL management is **NOT** required. 
- ⚡️️  **Plan & Apply**: Generate and view a plan with or without executing it against your cluster.
- 💻  **Portable**: Works across self-hosted clusters, managed clusters, and even Confluent Cloud clusters.
- 🦄  **Idempotency**: Executing the same desired state file on an up-to-date cluster will yield the same result.
- ☀️  **Continue from failures**: If a specific step fails during an apply, you can fix your desired state and re-run the command. You can execute `kafka-gitops` again without needing to rollback any partial successes.

## Getting Started

Start with the [installation and build instructions](docs/installation.md),
[quick start](docs/quick-start.md) and [documentation][documentation].

## Usage

Run `kafka-gitops` to view the help output.

```bash
Usage: kafka-gitops [-hvV] [--no-delete] [--skip-acls] [--skip-topics] [-c=<file>] [-f=<file>] [COMMAND]
Manage Kafka resources with a desired state file.
  -c, --command-config=<file>   Kafka client properties file.
  -f, --file=<file>   Specify the desired state file.
  -h, --help          Display this help message.
      --no-delete     Disable the ability to delete resources.
      --skip-acls     Do not take ACLs into account during plans or applies.
      --skip-topics   Do not take topics into account during plans or applies.
  -v, --verbose       Show more detail during execution.
  -V, --version       Print the current version of this tool.
Commands:
  account   Create Confluent Cloud service accounts.
  apply     Apply changes to Kafka resources.
  plan      Generate an execution plan of changes to Kafka resources.
  validate  Validates the desired state file.
```

`plan` includes replication-factor changes by default and only reads Kafka state.
Use `plan --no-manage-replication` to exclude RF changes; the old
`--manage-replication` flag remains a deprecated compatibility alias. Use
`plan --help` or `apply --help` for the RF and rack-policy options.

RF decreases are visible in the plan, but applying them requires a fresh
`apply --allow-replication-decrease`; `--no-delete` forbids decreases even with
that permission. Apply always rechecks RF safety against fresh metadata. A
blocked text preview is not exported as executable JSON. See the
[migration notes and capability limits](docs/design/development-task2-compatibility-rf-ux.md).

## Configuration

Configure Kafka through a standard properties file or environment variables:

```bash
java -jar build/libs/kafka-gitops-all.jar --command-config client.properties plan -o plan.json
```

Properties explicitly set in `--command-config` override the corresponding
environment values. Environment supplies properties absent from the file.
An unreadable or malformed file fails the command. Set `bootstrap.servers`
explicitly: there is no implicit `localhost:9092` fallback. Local `validate`
does not require a Kafka connection.

To configure properties, prefix them with `KAFKA_`. For example:

* `KAFKA_BOOTSTRAP_SERVERS`: Injects as `bootstrap.servers`
* `KAFKA_CLIENT_ID`: Injects as `client.id`

Additionally, we provide helpers for setting the `sasl.jaas.config` for clusters such as Confluent Cloud.

By setting:

* `KAFKA_SASL_JAAS_USERNAME`: Username to use
* `KAFKA_SASL_JAAS_PASSWORD`: Password to use

The following configuration is generated:

* `sasl.jaas.config`: `org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME" password="PASSWORD";`

## State File

By default, `kafka-gitops` looks for `state.yaml` in the current directory. You can also use `kafka-gitops -f` to pass a file.

An example desired state file:

```yaml
topics:
  example-topic:
    partitions: 6
    replication: 3
    configs:
      cleanup.policy: compact

services:
  example-service:
    type: application
    produces:
      - example-topic
    consumes:
      - example-topic
```

## Build and run

Use a JDK 21 installation through `JAVA_HOME` or `PATH`, then the Gradle Wrapper
(Gradle 9.9.1, Shadow 9.6.1). No system Gradle installation is required.

```bash
./gradlew --no-daemon --no-scan clean classes testClasses test shadowJar verifyReleaseArtifact buildExecutableJar buildRelease
java -jar build/libs/kafka-gitops-all.jar --version
java -jar build/libs/kafka-gitops-all.jar --help
```

On Windows use `.\gradlew.bat` for the same tasks. The first build downloads
dependencies from their configured repositories. Safe unit tests exclude the
external-cluster `*IntegrationSpec` suites; they do not start a Kafka lab.
Outputs are the universal `build/libs/kafka-gitops-all.jar`, POSIX launcher
`build/output/kafka-gitops`, and `build/distributions/kafka-gitops-0.4.0.zip`.
The JAR and ZIP contain component-specific licenses and attribution.
The standard `build`/`assemble` lifecycle uses these same verified artifacts.
Old thin/shadow distribution and install tasks are disabled; no generated
launcher references the disabled plain JAR.

For a downloaded release, verify its JAR against the SHA-256 published with
that exact release, then run it with Java 21:

```bash
sha256sum kafka-gitops-all.jar
java -jar kafka-gitops-all.jar --version
```

PowerShell equivalent: `Get-FileHash .\kafka-gitops-all.jar -Algorithm SHA256`.
The future fork's release URL is deliberately unset; upstream Homebrew,
container and release downloads do not distribute this independent 0.4.0 build.

No Kafka environment variables are mandatory when `--command-config` supplies
the required client settings. Keep credentials outside Git. See
[SECURITY.md](SECURITY.md) before using authentication settings or applying changes.

## Compatibility and safety limits

| Target | Live scenario evidence | New 0.4.0 packaging |
|---|---|---|
| Apache Kafka 3.2.3 | VERIFIED: topics/configs/partitions and RF 1→2→3→2 | Unit/CLI/packaging verification; live trials not repeated |
| Apache Kafka 3.7.1 | VERIFIED: topics/configs/partitions and RF 1→2→3→2 | Unit/CLI/packaging verification; live trials not repeated |
| Apache Kafka 4.4.0 | VERIFIED: topics/configs/partitions, RF and ELR scenarios | Unit/CLI/packaging verification; live trials not repeated |
| Arenadata Streaming / ADS 4.3.1 | **NOT TESTED LIVE** | No vendor compatibility claim |

The [recorded live tests](docs/testing/kafka-3x-compatibility-results.md) used
the preceding artifact; the release preparation report records the new hash.
Changes in this preparation concern version/legal packaging, notices, docs, CI
and runner path portability. The RF algorithm and ACL/DESCRIBE semantics are
unchanged. Old live results are not trials of the newly packaged binary.
Full SASL/TLS/Kerberos/OAuth and ACL enforcement coverage is not established.
Production readiness, every OS/CPU and all failure/load scenarios are not claimed.
An updated client does not patch vulnerabilities in old brokers.

`plan` reads and reports changes; only an explicit `apply` performs changes.
Review RF decreases and fresh preflight blockers. ELR/LastKnownELR unknown data
is not converted to an empty set; unsupported/unknown safety evidence blocks
unsafe RF execution. See [ELR details](docs/design/elr-replication-compatibility.md).

## Contributing

Contributions are very welcome. See [CONTRIBUTING.md][contributing] for details.

## License

Copyright (c) 2020 Shawn Seymour.

Licensed under the [Apache 2.0 license][license].

[documentation]: ./docs/documentation.md
[contributing]: ./CONTRIBUTING.md
[license]: ./LICENSE
