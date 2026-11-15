<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Confluent Cloud example fragments

These inherited YAML fragments illustrate the **legacy external `ccloud`
account-resolution path**. Confluent Cloud and current external-CLI compatibility
are not fully live-accepted for Kafka GitOps 0.4.0. See the current
[Confluent Cloud guide](../../docs/confluent-cloud.md) for explicit-principal
configuration, safety limits and authentication handling.

## Files and root state

- [services.yaml](services.yaml) defines a service whose principal is resolved
  from an external service account.
- [topics.yaml](topics.yaml) defines a topic and its configuration.
- [users.yaml](users.yaml) defines a user-role mapping; legacy external account
  names use the `user-{name}` prefix.

Create a root `state.yaml` next to those fragments only when deliberately using
the legacy integration:

```yaml
settings:
  ccloud:
    enabled: true
  files:
    services: services.yaml
    topics: topics.yaml
    users: users.yaml
```

Fragment paths are resolved relative to the root state file. With the
[Java 21 JAR](../../docs/installation.md) available in your working directory:

```bash
java -jar kafka-gitops-all.jar -f state.yaml validate
```

Local validation does not run the external CLI or contact Kafka. It does not
prove that a deployed `ccloud` version, authentication or vendor policy works.

## Legacy account action

The supported GitOps subcommand is `account`, singular. It can create external
service accounts, so run it only after separately authorizing and accepting the
legacy CLI/authentication path described in the guide:

```bash
java -jar kafka-gitops-all.jar -f state.yaml account
```

The former `accounts` spelling is not a CLI command. Do not reuse the old
password-based `ccloud login` automation as a verified setup procedure.

## Plan and apply

Prepare private Kafka client properties as described in the guide. A plan is
read-only; with legacy account resolution enabled it also lists external accounts:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --no-delete -f state.yaml plan -o plan.json
```

After a successful plan, review every topic and ACL operation before the separate
write step:

```bash
java -jar kafka-gitops-all.jar --command-config client.properties --no-delete apply -p plan.json
```

Apply can change Kafka and partially complete; it has no automatic rollback.
`--no-delete` forbids deletion and RF decreases. RF planning is enabled by
default and still requires the documented safety evidence. No credentials are
included here; keep client properties and sensitive evidence outside Git.
