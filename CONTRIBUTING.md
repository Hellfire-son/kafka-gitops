<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Contributing

Contribute to [Hellfire-son/kafka-gitops](https://github.com/Hellfire-son/kafka-gitops),
the independent community-maintained fork based on joschi/kafka-gitops 0.3.1.
The original project was created by Shawn Seymour. Fork-specific bugs and
proposals belong here; do not direct them to upstream maintainers by default.

## Issues and discussion

Use this fork's [Issues](https://github.com/Hellfire-son/kafka-gitops/issues) for
non-security bugs and proposals when the tracker is enabled.

**Repository setting checked on 2026-11-15: Issues are disabled.** The owner must
enable Issues to support the issue-first workflow below. Until then, use a
[draft Pull Request](https://github.com/Hellfire-son/kafka-gitops/pulls) for public
scope discussion. Do not publish secrets or undisclosed vulnerability details;
use the private channel in [SECURITY.md](SECURITY.md).

## Contribution workflow

1. Once Issues are enabled, open an Issue describing the problem or proposal,
   affected version and a sanitized reproduction. While disabled, provide that
   context in a draft PR instead.
2. Discuss substantial behavior, compatibility or security changes before
   implementing them.
3. Create a separate feature branch in your working clone or a personal fork
   if you do not have write access.
4. Make focused changes and explain the resulting behavior. Preserve compatible
   YAML/JSON formats and documented RF/ACL behavior unless a change is agreed.
5. Add or update tests that verify changed behavior. Documentation-only changes
   need appropriate link, example and rendering checks.
6. Run the safe checks below and inspect `git diff --check` and the changed paths.
7. Open a [Pull Request](https://github.com/Hellfire-son/kafka-gitops/pulls) targeting
   **`main` in `Hellfire-son/kafka-gitops`**. Describe the problem, resulting
   behavior, validation and remaining limitations; link the Issue when available.

The default branch was verified as `main`. These are contribution guidelines,
not a claim that GitHub branch protection enforces particular review rules.

## Requirements and safe checks

- Use **JDK 21** and the checked-in **Gradle Wrapper 9.9.1**; the build uses
  Shadow 9.6.1 and Kafka Client 4.4.0. No system Gradle installation is needed.
- Follow [installation and build](docs/installation.md#build-from-source) for safe
  Java unit tests, offline Python guards and packaged-JAR CLI/JNI verification.
- The Java test task excludes external-cluster `*IntegrationSpec` suites.
  Docker/Kafka integration runners, external accounts and cluster writes need
  separate acceptance authorization; they are not automatic unit-test steps.
- For documentation, check relative links, GitHub-compatible Markdown, YAML and
  CLI examples. Use local `validate`, `--help` and `--version` when sufficient.
- Do not include real client properties, credentials, tokens, keytabs, private
  keys, private paths, raw logs or unredacted scanner reports in Issues or PRs.
  If a secret was exposed, revoke/rotate it; deleting a file does not erase history.

## Licensing and attribution

Preserve the project Apache License 2.0, upstream copyright, modification notices
and third-party legal texts. Dependency or packaging changes require a fresh
review of their exact licenses and source-availability obligations. See
[provenance](CHANGES-FROM-UPSTREAM.md) and [third-party licensing](legal/README.md).

Original project: [devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops),
created by Shawn Seymour. Immediate base:
[joschi/kafka-gitops](https://github.com/joschi/kafka-gitops).
