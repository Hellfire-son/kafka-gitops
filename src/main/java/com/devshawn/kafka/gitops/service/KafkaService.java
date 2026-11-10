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
import org.apache.kafka.common.KafkaException;
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
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.time.Duration;

public class KafkaService {

    private final KafkaGitopsConfig config;

    public KafkaService(KafkaGitopsConfig config) {
        this.config = config;
    }

    public List<AclBinding> getAcls() {
        try (final Admin adminClient = buildAdminClient()) {
            ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
            AccessControlEntryFilter accessFilter = new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);
            AclBindingFilter filter = new AclBindingFilter(resourcePatternFilter, accessFilter);
            return new ArrayList<>(adminClient.describeAcls(filter).values().get());
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to list Kafka ACLs", ex.getMessage());
        }
    }

    public void createAcl(AclBinding aclBinding) {
        try (final Admin adminClient = buildAdminClient()) {
            adminClient.createAcls(Collections.singletonList(aclBinding)).all().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to create a Kafka ACL", ex.getMessage());
        }
    }

    public void deleteAcl(AclBinding aclBinding) {
        try (final Admin adminClient = buildAdminClient()) {
            adminClient.deleteAcls(Collections.singletonList(aclBinding.toFilter())).all().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to delete a Kafka ACL", ex.getMessage());
        }
    }

    public void createTopic(String topicName, TopicDetails topicDetails) {
        try (final Admin adminClient = buildAdminClient()) {
            short replicationFactor = topicDetails.getReplication().orElseThrow().shortValue();
            NewTopic newTopic = new NewTopic(topicName, topicDetails.getPartitions(), replicationFactor);
            newTopic.configs(topicDetails.getConfigs());
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (InterruptedException | ExecutionException | NoSuchElementException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to create a Kafka topic", ex.getMessage());
        }
    }

    public void deleteTopic(String topicName) {
        try (final Admin adminClient = buildAdminClient()) {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to delete a Kafka topic", ex.getMessage());
        }
    }

    public void updateTopicConfig(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        try (final Admin adminClient = buildAdminClient()) {
            adminClient.incrementalAlterConfigs(configs).all().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to update a Kafka topic config", ex.getMessage());
        }
    }

    public List<TopicListing> getTopics() {
        try (final Admin adminClient = buildAdminClient()) {
            Collection<TopicListing> topics = adminClient.listTopics().listings().get();
            return new ArrayList<>(topics);
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to list Kafka topics", ex.getMessage());
        }
    }

    public Map<ConfigResource, Config> describeConfigsForTopics(List<String> topicNames) {
        try (final Admin adminClient = buildAdminClient()) {
            List<ConfigResource> resources = topicNames.stream().map(it -> new ConfigResource(ConfigResource.Type.TOPIC, it)).toList();
            return adminClient.describeConfigs(resources).all().get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new KafkaExecutionException("Error thrown when attempting to describe a Kafka topic configuration", ex.getMessage());
        }
    }

    public Map<String, TopicDescription> describeTopics(List<String> names) {
        if (names.isEmpty()) {
            return Map.of();
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(partitionTimeoutMs());
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
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(partitionTimeoutMs());
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
        Object value = config.getConfig().getOrDefault(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        try {
            int timeout = Integer.parseInt(value.toString());
            if (timeout > 0) {
                return timeout;
            }
        } catch (NumberFormatException ignored) {
            // Do not include configuration values in validation errors.
        }
        throw new KafkaExecutionException("Invalid Kafka operation timeout.",
                "default.api.timeout.ms must be a positive integer for partition operations.");
    }

    private Admin partitionAdmin() {
        try {
            return buildAdminClient();
        } catch (RuntimeException ex) {
            throw partitionFailure("initializing the partition Admin client", ex);
        }
    }

    private static <T> T await(KafkaFuture<T> future, long deadline) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(remainingNanos(deadline), TimeUnit.NANOSECONDS);
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

    private static KafkaExecutionException partitionFailure(String operation, Throwable error) {
        Throwable cause = error instanceof ExecutionException && error.getCause() != null ? error.getCause() : error;
        // Kafka/server exception text may contain configuration values. Report only its category.
        return new KafkaExecutionException("Error while " + operation + ".", "Failure type: " + cause.getClass().getSimpleName()
                + ". No partition result was confirmed; changes may be partially applied. Re-plan before retrying.");
    }

    /** One read-only Admin session for the complete RF planning snapshot. */
    public ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names) {
        if (names.isEmpty()) {
            throw new IllegalArgumentException("An RF snapshot requires at least one topic.");
        }
        Admin admin = null;
        try {
            int timeout = partitionTimeoutMs();
            admin = buildAdminClient();
            return ReplicationMetadataReader.read(admin, new TreeSet<>(names), timeout);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaExecutionException("RF metadata collection interrupted.", "No RF changes were sent.");
        } catch (KafkaExecutionException ex) {
            // Reader diagnostics are fixed strings; buildAdminClient diagnostics may contain raw configuration.
            throw new KafkaExecutionException("RF metadata collection failed.", "Cannot confirm the complete RF snapshot. No RF changes were sent.");
        } catch (ExecutionException | TimeoutException | RuntimeException ex) {
            throw new KafkaExecutionException("RF metadata collection failed.", "Cannot confirm the complete RF snapshot. No RF changes were sent.");
        } finally {
            if (admin != null) {
                try {
                    admin.close(Duration.ZERO);
                } catch (RuntimeException ignored) {
                    // Do not replace safe diagnostics with a close exception containing external response text.
                }
            }
        }
    }

    protected Admin buildAdminClient() {
        try {
            return AdminClient.create(config.getConfig());
        } catch (KafkaException ex) {
            throw new KafkaExecutionException("Error thrown when creating Kafka admin client", ex.getCause().getMessage());
        }
    }
}
