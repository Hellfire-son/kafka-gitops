package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*;

/** All calls use the supplied session and one deadline. Never owns or creates an Admin client. */
final class ReplicationMetadataReader {
    private ReplicationMetadataReader() {
    }

    static ReplicationClusterSnapshot read(Admin admin, Set<String> requested, int timeoutMs)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(ms(deadline)));
        String clusterId = await(cluster.clusterId(), deadline);
        Map<Integer, Node> nodes = nodes(await(cluster.nodes(), deadline));
        Map<String, TopicDescription> descriptions = await(admin.describeTopics(new TreeSet<>(requested),
                new DescribeTopicsOptions().timeoutMs(ms(deadline))).allTopicNames(), deadline);
        require(descriptions.keySet().equals(requested), "RF topic metadata is incomplete.");

        Map<TopicPartition, PartitionReassignment> active = await(admin.listPartitionReassignments(
                new ListPartitionReassignmentsOptions().timeoutMs(ms(deadline))).reassignments(), deadline);
        List<ConfigResource> resources = new ArrayList<>();
        requested.stream().sorted().forEach(name -> resources.add(new ConfigResource(ConfigResource.Type.TOPIC, name)));
        nodes.keySet().forEach(id -> resources.add(new ConfigResource(ConfigResource.Type.BROKER, id.toString())));
        Map<ConfigResource, KafkaFuture<Config>> configs = admin.describeConfigs(resources,
                new DescribeConfigsOptions().includeSynonyms(true).timeoutMs(ms(deadline))).values();
        Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> directories = admin.describeLogDirs(nodes.keySet(),
                new DescribeLogDirsOptions().timeoutMs(ms(deadline))).descriptions();

        Map<Integer, Broker> brokers = new TreeMap<>();
        Map<Integer, Long> load = new TreeMap<>();
        for (Node node : nodes.values()) {
            Config config = optional(configs.get(new ConfigResource(ConfigResource.Type.BROKER, node.idString())), deadline);
            Observation uncordoned = uncordoned(config);
            Observation reachable = UNKNOWN;
            Observation storage = UNKNOWN;
            OptionalLong usable = OptionalLong.empty();
            try {
                KafkaFuture<Map<String, LogDirDescription>> future = directories.get(node.id());
                Map<String, LogDirDescription> dirs = future == null ? null : await(future, deadline);
                if (dirs != null && !dirs.isEmpty()) {
                    reachable = VERIFIED;
                    storage = dirs.values().stream().anyMatch(d -> d == null || d.error() != null) ? UNAVAILABLE : VERIFIED;
                    if (storage == VERIFIED) {
                        // Broker-scoped DescribeLogDirs is not the ACL-filtered topic listing.
                        // Count actual current replicas once; a future log-directory copy is not another replica.
                        Set<TopicPartition> current = new HashSet<>();
                        for (LogDirDescription dir : dirs.values()) {
                            for (var replica : dir.replicaInfos().entrySet()) {
                                require(replica.getKey() != null && replica.getValue() != null, "RF log directory replica metadata is invalid.");
                                if (!replica.getValue().isFuture()) {
                                    require(current.add(replica.getKey()), "RF log directory metadata duplicates a current replica.");
                                }
                            }
                        }
                        load.put(node.id(), (long) current.size());
                    }
                    if (storage == VERIFIED && dirs.values().stream().allMatch(d -> d.usableBytes().isPresent())) {
                        usable = OptionalLong.of(dirs.values().stream().mapToLong(d -> d.usableBytes().getAsLong()).min().orElseThrow());
                    }
                }
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof TimeoutException || cause instanceof BrokerNotAvailableException) {
                    reachable = UNAVAILABLE;
                } else if (cause instanceof KafkaStorageException) {
                    reachable = VERIFIED;
                    storage = UNAVAILABLE;
                }
                // Authorization/unsupported API/other failures remain UNKNOWN. Never expose the response text.
            }
            // Kafka 3.9 Node/DescribeClusterOptions cannot expose the 4.x fencing metadata.
            brokers.put(node.id(), new Broker(node.id(), Optional.ofNullable(node.rack()), reachable, UNKNOWN, storage, uncordoned, usable));
        }

        Map<String, Topic> topics = new TreeMap<>();
        for (String name : new TreeSet<>(requested)) {
            TopicDescription description = descriptions.get(name);
            Config config = optional(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name)), deadline);
            List<Partition> partitions = description.partitions().stream().sorted(Comparator.comparingInt(p -> p.partition()))
                    .map(p -> new Partition(p.partition(), p.replicas().stream().map(Node::id).toList(),
                            p.leader() == null || p.leader().id() < 0 ? OptionalInt.empty() : OptionalInt.of(p.leader().id()),
                            new HashSet<>(p.isr().stream().map(Node::id).toList()))).toList();
            topics.put(name, new Topic(description.topicId().toString(), partitions, minIsr(config)));
        }
        // Detect obvious drift during collection. This is not an atomic Kafka snapshot or an apply preflight.
        DescribeClusterResult after = admin.describeCluster(new DescribeClusterOptions().timeoutMs(ms(deadline)));
        require(Objects.equals(clusterId, await(after.clusterId(), deadline))
                && nodes.equals(nodes(await(after.nodes(), deadline))), "RF cluster metadata changed while collecting the snapshot. Re-plan.");
        Map<String, TopicDescription> fresh = await(admin.describeTopics(new TreeSet<>(requested),
                new DescribeTopicsOptions().timeoutMs(ms(deadline))).allTopicNames(), deadline);
        require(fresh.keySet().equals(requested) && requested.stream().allMatch(name -> sameTopic(descriptions.get(name), fresh.get(name))),
                "RF topic metadata changed while collecting the snapshot. Re-plan.");
        Map<TopicPartition, PartitionReassignment> finalActive = await(admin.listPartitionReassignments(
                new ListPartitionReassignmentsOptions().timeoutMs(ms(deadline))).reassignments(), deadline);
        require(active.keySet().equals(finalActive.keySet()), "Active reassignment changed during RF metadata collection. Re-plan.");
        return new ReplicationClusterSnapshot(clusterId, brokers, topics, active.keySet(), VERIFIED, load, VERIFIED);
    }

    private static boolean sameTopic(TopicDescription before, TopicDescription after) {
        if (before == null || after == null || !before.topicId().equals(after.topicId())
                || !before.name().equals(after.name()) || before.partitions().size() != after.partitions().size()) {
            return false;
        }
        return before.partitions().stream().allMatch(p -> after.partitions().stream().anyMatch(f ->
                p.partition() == f.partition() && Objects.equals(p.leader(), f.leader())
                        && p.replicas().stream().map(Node::id).toList().equals(f.replicas().stream().map(Node::id).toList())
                        && new HashSet<>(p.isr().stream().map(Node::id).toList()).equals(new HashSet<>(f.isr().stream().map(Node::id).toList()))));
    }

    private static Map<Integer, Node> nodes(Collection<Node> values) {
        Map<Integer, Node> result = new TreeMap<>();
        for (Node node : values) {
            require(node != null && node.id() >= 0 && result.put(node.id(), node) == null, "RF broker metadata is invalid.");
        }
        require(!result.isEmpty(), "RF broker metadata is empty.");
        return result;
    }

    private static Observation uncordoned(Config config) {
        ConfigEntry entry = config == null ? null : config.get("cordoned.log.dirs");
        if (entry == null || entry.isSensitive() || entry.value() == null) {
            return UNKNOWN;
        }
        // Conservative 3.9 fallback: any cordoned directory excludes the whole broker from new placement.
        return entry.value().isBlank() ? VERIFIED : UNAVAILABLE;
    }

    private static OptionalInt minIsr(Config config) {
        ConfigEntry entry = config == null ? null : config.get("min.insync.replicas");
        if (entry != null && !entry.isSensitive() && entry.value() != null) {
            try {
                int value = Integer.parseInt(entry.value());
                if (value > 0) {
                    return OptionalInt.of(value);
                }
            } catch (NumberFormatException ignored) {
                // Never propagate values of broker configs in error messages.
            }
        }
        return OptionalInt.empty();
    }

    private static Config optional(KafkaFuture<Config> future, long deadline)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        try {
            return future == null ? null : await(future, deadline);
        } catch (ExecutionException ignored) {
            return null;
        }
    }

    private static <T> T await(KafkaFuture<T> future, long deadline)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        return future.get(remaining(deadline), TimeUnit.NANOSECONDS);
    }

    private static long remaining(long deadline) throws java.util.concurrent.TimeoutException {
        long value = deadline - System.nanoTime();
        if (value <= 0) {
            throw new java.util.concurrent.TimeoutException();
        }
        return value;
    }

    private static int ms(long deadline) throws java.util.concurrent.TimeoutException {
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining(deadline)));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new KafkaExecutionException("Cannot collect RF planning metadata.", message);
        }
    }
}
