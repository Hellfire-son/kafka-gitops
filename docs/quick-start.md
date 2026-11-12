# Quick Start

Getting started with `kafka-gitops` is simple. For this tutorial, we will assume:

- You have installed the `kafka-gitops` command as [described here](/installation.md).
- You have a kafka cluster running on `localhost:9092`. 

!> **NOTE**: If you desire to use this with Confluent Cloud, read our [Confluent Cloud page][ccloud].

## Desired State File

First, create a desired state file named `state.yaml`. In this file, we'll define a topic. To showcase how to set topic configs, we'll make it a compacted topic.

```yaml
topics:
  my-example-topic:
    partitions: 6
    replication: 1
    configs:
      cleanup.policy: compact
```

## Configuration

Configure `kafka-gitops` through a Kafka client properties file or environment variables.
Properties explicitly present in `--command-config` override environment values;
environment fills properties absent from the file. An unreadable or malformed file
fails the command. There is no implicit bootstrap server.

For example, create `client.properties` containing `bootstrap.servers=localhost:9092`:

```bash
kafka-gitops --command-config client.properties plan -o plan.json
```

To configure properties through environment, prefix them with `KAFKA_`. For example:

* `KAFKA_BOOTSTRAP_SERVERS`: Injects as `bootstrap.servers`
* `KAFKA_CLIENT_ID`: Injects as `client.id`

For our quick start example, open a terminal where your `state.yaml` file is located and set the bootstrap servers:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

!> **NOTE**: If running `kafka-gitops` against a secured Kafka cluster, it must be run with super user credentials or a user with special ACLs. [Read more on our permissions page][permissions].

## Validate
We can validate the desired state file conforms to the [specification][specification]. To do this, run:

```bash
kafka-gitops validate
```

By default, it will look for `state.yaml` in the current directory. To specify a different file name, run:

```bash
kafka-gitops -f my-state-file.yaml validate
```

An example success message would look like:

```text
[VALID] Successfully validated the desired state file.
```

## Plan

We're now ready to generate a plan to execute against the cluster. By using the plan command, we are **NOT** changing the cluster.

Generate a plan by running:

```bash
kafka-gitops plan
```

For a newly created topic, an abbreviated text plan looks like:


```text
[CONFIG] Loading Kafka properties ...
[CONNECT] Connecting to the explicitly configured Kafka bootstrap servers ...
[METADATA] Reading Kafka resource and broker metadata ...
[PLAN] Calculating and summarizing changes ...
[PLAN] Topic: my-example-topic
  Partitions: ABSENT -> 6
  Replication: ABSENT -> 1
  Config cleanup.policy: ABSENT -> compact
[SUMMARY]
  Proposed executable resource changes: 1
  Status: PLAN_BUILT
  Ready for apply: FRESH_PREFLIGHT_REQUIRED
```

RF is managed by default for existing topics too. To retain the earlier behavior
of ignoring RF drift, use `kafka-gitops plan --no-manage-replication`. The old
`--manage-replication` remains a deprecated alias. Opposite flags together fail.
Use `plan --help` to inspect placement options. `RACK_AWARE` is the default;
`--replication-rack-policy BROKER_ONLY` explicitly accepts the rack-protection risk.

Existing topics show current and proposed ordered replicas, broker/rack facts
and reasons for exclusion. Unknown values are printed as `UNKNOWN`. Warnings
describe extra operational risks; critical unknowns and unsafe conditions make
the status `PLAN_BLOCKED`, return exit code 2 and prevent JSON export. Known
changes remain visible. Exit code 0 covers `NO_CHANGES` and `PLAN_BUILT`.

In most cases, you will want to output the plan to a file which can then be passed to the apply command. Plan files are `JSON` files. This can be done by running:

```bash
kafka-gitops plan -o plan.json
```

If running against a Kafka cluster with no authorizer configured or if you simply want to only manage topics, you can ignore ACLs completely. This can be done by running:

```bash
kafka-gitops --skip-acls plan
```

Conversely, if you're only interested in managing ACLs you can ignore changes to topics completely. This can be done by running:

```bash
kafka-gitops --skip-topics plan
```

## Apply

To execute a plan against the cluster, we use the apply command.

!> **WARNING**: This will apply changes to the cluster. This can be potentially destructive if you do not have all topics and ACLs defined.

Run:

```bash
kafka-gitops apply
```

By default, the apply command will generate a plan and then apply it. For most situations, **you should output a plan file from the plan command and pass it to the apply command**.

Example:

```bash
kafka-gitops apply -p plan.json
```

An RF decrease appears in `plan` without prior permission, but execution requires
`kafka-gitops apply -p plan.json --allow-replication-decrease`. Global `--no-delete`
forbids the decrease even with this permission. Permissions are never saved in
JSON. Applying an RF plan with `--no-manage-replication` fails before any writes,
including when both resource categories are skipped. Every RF apply performs
fresh identity, assignment, leader/ISR, min ISR, broker and conflict checks and
observes completion before reporting success. No automatic rollback occurs.

Automation that previously ignored RF drift should explicitly opt out when
planning. Text output has changed; use JSON for machine input and check the exit
code before reusing an output file. A blocked plan preserves any existing file;
an older file must not be treated as the output of a failed command. See the
[compatibility report](design/development-task2-compatibility-rf-ux.md) for examples
and the limits of unit-only compatibility evidence.

An output of a successful apply would look like:

```text
Executing apply...

Applying: [CREATE]

+ [TOPIC] my-example-topic
	+ cleanup.policy: compact

Successfully applied.

[SUCCESS] Apply complete! Resources: 1 created, 0 updated, 0 deleted.
```

If there is a partial failure, successes will not be rolled back. A failure error would look something like:

```text
[ERROR] KAFKA_ERROR: Creating a Kafka topic
State: KAFKA_ERROR. Failure type: PolicyViolationException.
External exception text omitted. Verify Kafka state before retrying; changes may be partially applied.
```

Congrats! You've successfully started using GitOps strategies to manage your cluster. If you have security on your cluster, read the [services][services] page to start defining services. 

[ccloud]: /confluent-cloud.md
[permissions]: /permissions.md
[specification]: /specification.md
[services]: /services.md
