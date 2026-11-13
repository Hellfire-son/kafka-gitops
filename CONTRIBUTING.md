<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Contributing

This is an independent community-maintained fork of joschi/kafka-gitops 0.3.1,
originally created by Shawn Seymour. The new fork URL has not been assigned.
Use the owner's confirmed repository after publication; do not direct new-fork
issues or pull requests to upstream maintainers by default.

## Feature Requests & Bug Reports
Use the issue tracker designated by the owner after the fork is published.
For security reports follow [SECURITY.md](SECURITY.md); no private reporting
address is invented here.

## Pull Requests
Base changes on the confirmed independent repository. Preserve upstream
copyright, license and provenance. Mark modified upstream files and retain
component-specific legal texts when changing runtime dependencies.

1. Discuss your proposed change in a GitHub issue first before spending time and implementing a feature or fix.

2. Ensure all changes are relevant to the pull request. Keep pull requests as small and to-the-point as possible.

3. Add & modify tests as necessary. Also, ensure the code matches the existing style.

4. Build with JDK 21 and the Gradle Wrapper using the safe commands in
   [installation](docs/installation.md). Run offline integration-runner guards
   and final-JAR CLI/JNI checks. External-cluster IntegrationSpec suites and
   lab write runners require separate authorization and are not automatic CI.
5. Open a pull request against the confirmed fork's main branch after publication.


Original project: [devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops).
Immediate base: [joschi/kafka-gitops](https://github.com/joschi/kafka-gitops).
