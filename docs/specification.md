<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Desired State Specification

This document describes the specification for how to write your Kafka cluster's desired state file. This currently must be a `YAML` file.

This reference describes the desired-state YAML accepted by **Kafka GitOps 0.4.1**.
It does not introduce a new YAML or executable-plan schema. See the
[release notes](release/kafka-gitops-v0.4.1-release-notes-en.md) for application
version, compatibility and migration.

The desired state file consists of:

- **Settings** [Optional]: Specific settings for configuring `kafka-gitops`.
- **Topics** [Optional]: Topic and topic configuration definitions.
- **Services** [Optional]: Service definitions for generating ACLs.
- **Users** [Optional]: User definitions for generating ACLs.
- **Custom Service ACLs** [Optional]: Definitions for custom, non-generated ACLs.
- **Custom User ACLs** [Optional]: Definitions for custom, non-generated ACLs.

## Settings

**Synopsis**: These are specific settings for configuring `kafka-gitops`.

**Options**:

- **ccloud** [Optional]: `enabled: true` selects the legacy external `ccloud`
  account-resolution integration, whose current vendor/CLI compatibility is
  unverified. Leave unset or false when using explicit principals. See
  [Confluent Cloud](confluent-cloud.md).
- **services** [Optional]: `acls.describeTopicEnabled: true` explicitly adds
  topic DESCRIBE to service profiles; it is not enabled automatically. See
  [services](services.md).
- **files** [Optional]: `services`, `topics` and `users` select additional YAML
  files, resolved relative to the root desired-state file.
- **topics** [Optional]:
    - **defaults** [Optional]: Specify topic defaults so you don't need to specify them for every topic in the state file. Currently, only replication is supported.
    - **exclude** [Optional]: Add a prefixed topic list for excluding specific topics when using `kafka-gitops`. This allows topics to be ignored from being deleted if they are not defined in the desired state file.
    - **include** [Optional]: Add a prefixed topic list for including specific topics when using `kafka-gitops`. This allows topics to be exclusively handled and topics not on the list are being ignored, even if they are not defined in the desired state file.

> `topics.exclude` and `topics.include` are _not mutually exclusive_ and can be used together to include specific topic prefixes and exclude individual "sub-topics".

> The exclude list takes precedence over the include list, so if a topic name is matched by both, it will be ignored and not deleted if it was not defined in the desired state file.

**Example**:
```yaml
settings:
  topics:
    defaults:
      replication: 3
    exclude:
      prefixed:
        - _confluent
        - my-topics-excluded
    include:
      prefixed:
        - my-topics
```

## Topics

**Synopsis**: Define the topics you would like on your cluster and their configuration.

> Each topic is defined as a key-value pair, with the key being the topic name and the value being an object of settings.

**Example**:

```yaml
topics:
  my-topic-name:
    partitions: 6
    replication: 3
    configs:
      cleanup.policy: compact
      segment.bytes: 1000000
```

If a default `replication` value is supplied in the `settings` block, then the `replication` field can be omitted. If a default `replication` value is provided and the `replication` field in the topic definition is set, the default will be overridden for that topic.

For existing topics, partition increases and RF changes are supported; partition
decreases are not. RF is included in `plan` by default. Decreases require separate
`apply --allow-replication-decrease`, no `--no-delete` and fresh safety checks.
See [Quick Start](quick-start.md#replication-options).

## Services

**Synopsis**: Define the services that will utilize your Kafka cluster. These service definitions allow `kafka-gitops` to generate ACLs for you. Yay!

> Each service has a `type`. This defines its structure.

There are currently three service types:

- `application`
- `kafka-connect`
- `kafka-streams`

> **Note:** Omit `principal` only with the deliberately enabled legacy
> `settings.ccloud.enabled` account-resolution path. Explicit-principal usage
> leaves that setting unset or false. See [Confluent Cloud](confluent-cloud.md).

**Example application**:

> **NOTE**: The `group-id` property is optional and defaults to the service name.

```yaml
services:
  my-application-name:
    type: application
    principal: User:my-application-principal
    group-id: optional-group-id-override
    produces:
      - topic-name-one
    consumes:
      - topic-name-two
      - topic-name-three
```

**Example kafka connect cluster**:

> **NOTE**: The `group-id` property is optional and defaults to the service name. The `storage-topics` property is also optional; the defaults can be found on the [services][services] page.

```yaml
services:
  my-kafka-connect-cluster-name:
    type: kafka-connect
    principal: User:my-connect-principal
    group-id: optional-group-id-override
    storage-topics:
      config: optional-custom-config-topic
      offset: optional-custom-offset-topic
      status: optional-custom-status-topic
    connectors:
      my-source-connector-name:
        produces:
          - topic-name-one
      my-sink-connector-name:
        consumes:
          - topic-name-two
```

**Example kafka streams application**:

> **NOTE**: The `application-id` property is optional and defaults to the service name.

```yaml
services:
  my-kafka-streams-name:
    type: kafka-streams
    principal: User:my-streams-principal
    application-id: optional-application-id-override
    produces:
      - topic-name-one
    consumes:
      - topic-name-two
```

Behind the scenes, `kafka-gitops` generates ACLs based on these definitions.

## Users

**Synopsis**: Define the users that will utilize your Kafka cluster. These user definitions allow `kafka-gitops` to generate ACLs for you. Yay!

> **Note:** In the legacy `ccloud` account-resolution path, external accounts for
> YAML users are named with the `user-` prefix. This does not apply to explicit
> principal mappings with `settings.ccloud.enabled` unset or false.

```yaml
users:
  my-user:
    principal: User:my-user
    roles:
      - writer
      - reader
      - operator
```

Currently, three predefined roles exist:

- **writer**: access to write to all topics
- **reader**: access to read all topics using any consumer group
- **operator**: cluster/topic DESCRIBE, topic DESCRIBE_CONFIGS and consumer-group
  READ/DESCRIBE. This role alone does not grant topic READ.

Outside of these very simple roles, you can define custom ACLs per-user by using the `customUserAcls` block.


## Custom Service ACLs

**Synopsis**: Define custom ACLs for a specific service.

For example, if a specific application needs to read all topics prefixed with `kafka.` and `service.`, you may not want to define them all in your desired state file.

If you have a service `my-test-service` defined, you can define custom ACLs as so:

```yaml
customServiceAcls:
  my-test-service:
    read-all-kafka:
      name: kafka.
      type: TOPIC
      pattern: PREFIXED
      host: "*"
      principal: User:my-test-service
      operation: READ
      permission: ALLOW
    read-all-service:
      name: service.
      type: TOPIC
      pattern: PREFIXED
      host: "*"
      principal: User:my-test-service
      operation: READ
      permission: ALLOW
```

## Custom User ACLs

**Synopsis**: Define custom ACLs for a specific user.

For example, if a specific user needs to read all topics prefixed with `kafka.` and `service.`, you may not want to define them all in your desired state file.

If you have a user `my-test-user` defined, you can define custom ACLs as so:

```yaml
customUserAcls:
  my-test-user:
    read-all-kafka:
      name: kafka.
      type: TOPIC
      pattern: PREFIXED
      host: "*"
      operation: READ
      permission: ALLOW
```

> **NOTE**: The `principal` field can be left out here and it will be inherited from the user definition.

[services]: services.md
