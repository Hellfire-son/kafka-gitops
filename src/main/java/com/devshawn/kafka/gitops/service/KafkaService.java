// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.time.Duration;

public class KafkaService {

    private final KafkaGitopsConfig config;
    private final ThreadLocal<Long> planningDeadline = new ThreadLocal<>();

    public KafkaService(KafkaGitopsConfig config) {
        this.config = config;
    }

    public String connectionTarget() {
        return com.devshawn.kafka.gitops.util.KafkaDiagnostics.bootstrap(config.getConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    public List<AclBinding> getAcls() {
        ResourcePatternFilter resource = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
        AccessControlEntryFilter access = new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);
        return new ArrayList<>(request("Listing Kafka ACLs", admin -> admin.describeAcls(new AclBindingFilter(resource, access)).values()));
    }

    public void createAcl(AclBinding binding) {
        request("Creating a Kafka ACL", admin -> admin.createAcls(Collections.singletonList(binding)).all());
    }

    public void deleteAcl(AclBinding binding) {
        request("Deleting a Kafka ACL", admin -> admin.deleteAcls(Collections.singletonList(binding.toFilter())).all());
    }

    public void createTopic(String name, TopicDetails details) {
        NewTopic topic = new NewTopic(name, details.getPartitions(), details.getReplication().orElseThrow().shortValue());
        topic.configs(details.getConfigs());
        request("Creating a Kafka topic", admin -> admin.createTopics(Collections.singletonList(topic)).all());
    }

    public void deleteTopic(String name) {
        request("Deleting a Kafka topic", admin -> admin.deleteTopics(Collections.singletonList(name)).all());
    }

    public void updateTopicConfig(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        request("Updating Kafka topic configuration", admin -> admin.incrementalAlterConfigs(configs).all());
    }

    public List<TopicListing> getTopics() {
        return new ArrayList<>(request("Listing Kafka topics", admin -> admin.listTopics().listings()));
    }

    public Map<ConfigResource, Config> describeConfigsForTopics(List<String> names) {
        if (names.isEmpty()) return Map.of();
        List<ConfigResource> resources = names.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList();
        return request("Reading Kafka topic configurations", admin -> admin.describeConfigs(resources).all());
    }

    /** One command-wide budget for the read-only planning phase, in addition to each API timeout. */
    public void beginPlanning() { planningDeadline.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(partitionTimeoutMs())); }
    public void endPlanning() { planningDeadline.remove(); }

    private <T> T request(String operation, java.util.function.Function<Admin, KafkaFuture<T>> call) {
        try {
            long deadline = deadline();
            remainingNanos(deadline);
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            Admin admin = buildAdminClient();
            try { return await(call.apply(admin), deadline); }
            finally { admin.close(Duration.ZERO); }
        } catch (InterruptedException | ExecutionException | TimeoutException | RuntimeException ex) {
            throw operationFailure(operation, ex);
        }
    }

    private long deadline() {
        long value = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(partitionTimeoutMs());
        Long shared = planningDeadline.get();
        return shared == null ? value : Math.min(value, shared);
    }

    public Map<String, TopicDescription> describeTopics(List<String> names) {
        if (names.isEmpty()) {
            return Map.of();
        }
        long deadline = deadline();
        Admin admin = partitionAdmin();
        try {
            try {
                return await(admin.describeTopics(names, new DescribeTopicsOptions().timeoutMs(remainingMs(deadline)))
                        .allTopicNames(), deadline);
            } finally {
                admin.close(Duration.ZERO);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw partitionFailure("describing topic partitions", ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw partitionFailure("describing topic partitions", ex);
        }
    }

    /** Returns only after observing the target count. No-op also requires a fresh metadata read. */
    public boolean increaseTopicPartitions(String name, int previousCount, int targetCount) {
        if (previousCount < 1 || targetCount <= previousCount) {
            throw new IllegalArgumentException("Partition changes require a positive previous count and a larger target count.");
        }
        long deadline = deadline();
        Admin admin = partitionAdmin();
        try {
            try {
                int actual = partitionCount(admin, name, deadline);
                if (actual == targetCount) {
                    return false;
                }
                if (actual != previousCount) {
                    throw new KafkaExecutionException("Partition plan is stale.", "Topic " + name + " has " + actual
                            + " partitions, expected " + previousCount + ". Re-plan before retrying.");
                }
                await(admin.createPartitions(Collections.singletonMap(name, NewPartitions.increaseTo(targetCount)),
                        new CreatePartitionsOptions().timeoutMs(remainingMs(deadline))).all(), deadline);
                // Controller acknowledgement can precede metadata propagation to the broker answering describeTopics.
                while (true) {
                    actual = partitionCount(admin, name, deadline);
                    if (actual == targetCount) {
                        return true;
                    }
                    if (actual != previousCount) {
                        throw new KafkaExecutionException("Partition result differs from the plan.", "Topic " + name + " has "
                                + actual + " partitions, expected " + targetCount + ". Changes may be partially applied; re-plan before retrying.");
                    }
                    TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(100), remainingNanos(deadline)));
                }
            } finally {
                admin.close(Duration.ZERO);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw partitionFailure("increasing partitions for topic " + name, ex);
        } catch (KafkaExecutionException ex) {
            throw ex;
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw partitionFailure("increasing or verifying partitions for topic " + name, ex);
        }
    }

    private int partitionCount(Admin admin, String name, long deadline) throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, TopicDescription> descriptions = await(admin.describeTopics(Collections.singletonList(name),
                new DescribeTopicsOptions().timeoutMs(remainingMs(deadline))).allTopicNames(), deadline);
        TopicDescription topic = descriptions.get(name);
        if (topic == null || topic.partitions().isEmpty()) {
            throw new KafkaExecutionException("Cannot confirm partition count.", "Missing metadata for topic " + name
                    + ". Changes may be partially applied; re-plan before retrying.");
        }
        return topic.partitions().size();
    }

    private int partitionTimeoutMs() {
        Object value = config.getConfig().getOrDefault(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
        try {
            int timeout = Integer.parseInt(value.toString());
            if (timeout > 0) {
                return timeout;
            }
        } catch (NumberFormatException ignored) {
            // Do not include configuration values in validation errors.
        }
        throw new KafkaExecutionException("Invalid Kafka operation timeout.",
                "Resource: default.api.timeout.ms; actual: INVALID; required: a positive integer in milliseconds.",
                KafkaExecutionException.Code.INVALID_TIMEOUT);
    }

    private Admin partitionAdmin() {
        try {
            return buildAdminClient();
        } catch (RuntimeException ex) {
            throw partitionFailure("initializing the partition Admin client", ex);
        }
    }

    private static <T> T await(KafkaFuture<T> future, long deadline) throws InterruptedException, ExecutionException, TimeoutException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        try { return future.get(remainingNanos(deadline), TimeUnit.NANOSECONDS); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw ex; }
    }

    private static long remainingNanos(long deadline) throws TimeoutException {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new TimeoutException();
        }
        return remaining;
    }

    private static int remainingMs(long deadline) throws TimeoutException {
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos(deadline)));
    }

    private KafkaExecutionException partitionFailure(String operation, Throwable error) {
        return operationFailure(operation, error);
    }

    /** One read-only Admin session for the complete RF planning snapshot. */
    public ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names) {
        return readReplicationSnapshot(names, false);
    }

    public ReplicationClusterSnapshot readReplicationPreflightSnapshot(Set<String> names) {
        return readReplicationSnapshot(names, true);
    }

    /** Lazy production binding; constructing it performs no Kafka requests. */
    public ReplicationApplication replicationApplication() {
        return new AdminReplicationApplication(this::buildAdminClient);
    }

    private ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names, boolean preflight) {
        if (names.isEmpty()) {
            throw new IllegalArgumentException("An RF snapshot requires at least one topic.");
        }
        Admin admin = null;
        try {
            int timeout = remainingMs(deadline());
            admin = buildAdminClient();
            return ReplicationMetadataReader.read(admin, new TreeSet<>(names), timeout, preflight);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw operationFailure("Reading RF metadata", ex);
        } catch (KafkaExecutionException ex) {
            // Reader diagnostics are fixed strings; buildAdminClient diagnostics may contain raw configuration.
            throw operationFailure("Reading RF metadata", ex);
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw operationFailure("Reading RF metadata", ex);
        } finally {
            if (admin != null) {
                boolean interrupted = Thread.currentThread().isInterrupted();
                try {
                    admin.close(Duration.ZERO);
                } catch (RuntimeException ignored) {
                    // Do not replace safe diagnostics with a close exception containing external response text.
                } finally { if (interrupted) Thread.currentThread().interrupt(); }
            }
        }
    }

    protected Admin buildAdminClient() {
        Object bootstrap = config.getConfig().get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrap == null || bootstrap.toString().isBlank() || bootstrap.toString().equals("[]")) {
            throw new KafkaExecutionException("Kafka connection configuration is missing.",
                    "Resource: bootstrap.servers; actual: UNKNOWN (not configured); required: an explicit bootstrap server. "
                            + "Set --command-config or KAFKA_BOOTSTRAP_SERVERS. No connection was attempted.",
                    KafkaExecutionException.Code.BOOTSTRAP_MISSING);
        }
        try {
            Map<String, Object> properties = new java.util.HashMap<>(config.getConfig());
            int timeout = partitionTimeoutMs();
            properties.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeout);
            properties.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(10000, timeout));
            return AdminClient.create(properties);
        } catch (RuntimeException ex) {
            throw operationFailure("Creating Kafka Admin client", ex);
        }
    }

    private KafkaExecutionException operationFailure(String message, Throwable error) {
        var failure = com.devshawn.kafka.gitops.util.KafkaDiagnostics.failure(message, error);
        return new KafkaExecutionException(failure.getMessage(), failure.getExceptionMessage()
                + " Connection target: " + connectionTarget() + ".", failure.getCode());
    }
}
