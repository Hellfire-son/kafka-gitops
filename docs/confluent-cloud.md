<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Confluent Cloud and legacy account integration

Kafka GitOps 0.4.0 uses Kafka Admin APIs for topic/configuration and ACL operations.
**Confluent Cloud, its full security matrix and ACL enforcement have not received
complete live integration acceptance for this release.** The isolated Apache
Kafka trials are not evidence of vendor certification.

Install the Java 21 [universal JAR](installation.md) from the
[v0.4.0 release](https://github.com/Hellfire-son/kafka-gitops/releases/tag/v0.4.0).
Upstream Docker images are not the independent v0.4.0 distribution.

## Kafka client configuration

Use a private `client.properties`. This template contains only non-secret
settings and a placeholder bootstrap address:

```properties
bootstrap.servers=broker.example.invalid:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
ssl.endpoint.identification.algorithm=HTTPS
```

Replace the placeholder and supply the required authentication properties,
including `sasl.jaas.config` when required, through your approved private
credential configuration. The template is incomplete until those properties
are provided. Do not commit API keys, secrets or private client properties.

Use `--command-config client.properties`; no Kafka environment variables are
mandatory when the file supplies all required settings. File values override
corresponding environment properties. This documents configuration handling,
not a verified Confluent Cloud authentication recipe.

## Explicit principals

With explicitly provisioned principals, leave `settings.ccloud.enabled` unset
or false and use the normal [service definitions](services.md). For example,
replace `User:REPLACE_WITH_ACCOUNT_ID` with a principal valid for your environment:

```yaml
topics:
  example-topic:
    partitions: 3
    replication: 3

services:
  example-service:
    type: application
    principal: User:REPLACE_WITH_ACCOUNT_ID
    produces:
      - example-topic
```

The application profile generates a topic WRITE ACL; additional topic DESCRIBE
is opt-in. Review the generated ACLs against your actual authorizer and account
permissions. Managed services may restrict RF changes or broker metadata APIs;
missing critical evidence still blocks RF execution. Deliberately excluding RF
requires `plan --no-manage-replication`, not a safety bypass.

## Validate, plan, review, apply

Save the desired state as `state.yaml`. Validate without a connection:

```bash
java -jar kafka-gitops-all.jar -f state.yaml validate
```

After configuring and separately accepting the target environment, generate a
read-only plan with deletion disabled:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --no-delete -f state.yaml plan -o plan.json
```

Check the exit code and every topic/ACL operation before explicitly applying:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --no-delete apply -p plan.json
```

> **Warning:** Apply changes Kafka and may partially complete. Without
> `--no-delete`, resources omitted from desired state may be deleted. A blocked
> plan does not replace an older JSON file. No automatic rollback is provided.

## Legacy ccloud account integration

Setting `settings.ccloud.enabled: true` selects the existing external `ccloud`
principal-resolution path. Omit service/user principals only when deliberately
using this path. Kafka GitOps invokes `ccloud service-account list -o json` and,
for account creation, `ccloud service-account create ... -o json`. The executable
is selected by `CCLOUD_EXECUTABLE_PATH`, or the command `ccloud` on `PATH`.

The GitOps subcommand is **`account`**, singular:

```bash
java -jar kafka-gitops-all.jar -f state.yaml account
```

This is a separate account-creation action, not a read-only planning step. The
external CLI's current syntax, login and response-format compatibility have not
been verified for this release. Do not assume a modern `confluent` CLI can simply
replace `ccloud`, or rely on the former password-based CI login examples. Before
using this legacy path, establish a compatible CLI and private authentication in
a separately authorized acceptance environment.

See the [legacy example fragments](../examples/confluent-cloud/README.md),
[permissions](permissions.md), [specification](specification.md) and
[security policy](../SECURITY.md). No account or Kafka operations are run by
reading or locally validating these examples.
