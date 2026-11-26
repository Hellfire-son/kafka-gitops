<!-- Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md. -->
# Permissions

Kafka GitOps 0.4.1 needs Kafka authorization for every read and write operation
in its chosen scope. A dedicated principal can be granted the required rights;
superuser access is not mandatory for every deployment. Full authorization and
ACL enforcement acceptance has not been completed for the release.

Kafka GitOps manages ACL definitions, not Kafka authentication credentials.
External account creation is a separate [legacy Confluent Cloud](confluent-cloud.md)
integration. Existing READ/WRITE/CREATE and opt-in DESCRIBE profiles are unchanged.

## RF metadata and scope

RF planning/apply additionally uses cluster/topic identity, broker configs,
features, log-directory metadata and reassignment APIs. The inherited ACL
bootstrap examples below do not establish a complete RF permission matrix or
a least-privilege policy for every broker/vendor distribution. Verify the actual
required permissions and authorizer behavior for your deployment. Missing
critical evidence or authorization blocks RF execution.

`--skip-acls` excludes ACL management; it does not grant missing Kafka permissions.
`--skip-topics` excludes topic management. Plans and saved JSON do not grant rights.
See [Quick Start](quick-start.md), [RF safety](design/elr-replication-compatibility.md)
and [SECURITY.md](../SECURITY.md).

## Example

For the purposes of this example, we'll assume we have a user principal named `gitops-user`.

If ACL management is enabled, the `gitops-user` principal needs authority to
create and delete ACLs. User definitions in YAML generate ACLs for principals;
they do not provision broker authentication users.

If you do not want to use a super user, you can create a `gitops-user` principal and a current super user can make them an *ACL Administrator*. An ACL administrator has the `ALTER --cluster` access control entry. This entry allows the user to create and delete ACLs for the given cluster.

> **Caution**: An ACL administrator can then create ACLs for any other principal, including themselves.

### Manually Add ACLs

The `kafka-acls` examples below use Kafka administrative tooling supplied by your
broker distribution, not the GitOps JAR. Select the appropriate wrapper for your
installation. These are broad, mutating bootstrap examples; review them before
running with a separately authorized administrator. They are not a fully accepted
production permission recipe, especially for RF management.

Add the alter cluster ACL to the `gitops-user` principal:

```bash
kafka-acls --bootstrap-server localhost:9092 --command-config admin.properties \
--add --allow-principal User:gitops-user \
--operation  ALTER --cluster
```

Add the ACLs needed to manage topics to the `gitops-user` principal:

```bash
kafka-acls --bootstrap-server localhost:9092 --command-config admin.properties --add \
--allow-principal User:gitops-user --operation Create --operation Delete \
--operation DescribeConfigs --operation AlterConfigs --operation Alter \
--operation Describe --topic '*'
```

These examples illustrate ACL, topic and topic-configuration permissions. They
do not prove that all RF metadata reads or vendor-specific policies are authorized.

### State File Definition

The YAML below is an ACL bootstrap example. Use a separately authorized
administrator's private `admin.properties`, review the generated ACLs and retain
`--skip-topics --no-delete` to exclude topic management and prevent removals.
Save it as `acl-bootstrap.yaml`, then plan and explicitly apply only after review:

```bash
java -jar kafka-gitops-all.jar --command-config admin.properties --skip-topics --no-delete -f acl-bootstrap.yaml plan -o acl-bootstrap-plan.json
java -jar kafka-gitops-all.jar --command-config admin.properties --skip-topics --no-delete apply -p acl-bootstrap-plan.json
```

These commands can grant broad access. Validate the resulting permissions before
switching to the `gitops-user` principal; the example is not full RF acceptance.

```yaml
users:
  gitops-user:
    principal: User:gitops-user

customUserAcls:
  gitops-user:
    alter-cluster:
      name: kafka-cluster
      type: CLUSTER
      pattern: LITERAL
      host: "*"
      operation: ALTER
      permission: ALLOW
    create-topics:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: CREATE
      permission: ALLOW
    alter-topics:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: ALTER
      permission: ALLOW
    describe-topics:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: DESCRIBE
      permission: ALLOW
    delete-topics:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: DELETE
      permission: ALLOW
    describe-topic-configs:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: DESCRIBE_CONFIGS
      permission: ALLOW
    alter-topic-configs:
      name: "*"
      type: TOPIC
      pattern: LITERAL
      host: "*"
      operation: ALTER_CONFIGS
      permission: ALLOW
```

