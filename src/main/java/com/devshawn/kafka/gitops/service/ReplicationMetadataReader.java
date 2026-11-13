// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence;
import com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment;
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.Capability.*;
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
        return read(admin, requested, timeoutMs, false);
    }

    static ReplicationClusterSnapshot read(Admin admin, Set<String> requested, int timeoutMs, boolean preflight)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        FeatureMetadata features = features(admin, deadline);
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

        Map<Integer, Config> firstBrokerConfigs = new TreeMap<>();
        Map<Integer, Broker> brokers = new TreeMap<>();
        Map<Integer, Long> load = new TreeMap<>();
        for (Node node : nodes.values()) {
            Config config = optional(configs.get(new ConfigResource(ConfigResource.Type.BROKER, node.idString())), deadline);
            firstBrokerConfigs.put(node.id(), config);
            CapabilityAssessment cordon = KafkaCapabilities.cordoning(config, features);
            Observation uncordoned = cordoningObservation(cordon);
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
            // The default broker-endpoint DescribeCluster response excludes fenced brokers on old
            // protocols too (KIP-1073). This is an API contract, not an inference from a missing field.
            boolean zooKeeper = KafkaCapabilities.zooKeeper(config);
            Observation unfenced = node.isFenced() ? UNAVAILABLE : zooKeeper ? NOT_APPLICABLE : VERIFIED;
            Map<CapabilityAssessment.Capability, CapabilityAssessment> capabilities = new EnumMap<>(CapabilityAssessment.Capability.class);
            capabilities.put(FENCING, new CapabilityAssessment(node.isFenced() ? CapabilityAssessment.State.UNSAFE
                    : zooKeeper ? CapabilityAssessment.State.NOT_APPLICABLE : CapabilityAssessment.State.SUPPORTED,
                    node.isFenced() ? CapabilityAssessment.Reason.BROKER_FENCED
                    : zooKeeper ? CapabilityAssessment.Reason.ZOOKEEPER_MODE_CONFIRMED : CapabilityAssessment.Reason.LIVE_BROKER_METADATA, true));
            capabilities.put(CORDONING, cordon);
            capabilities.put(STORAGE, new CapabilityAssessment(storage == VERIFIED ? CapabilityAssessment.State.SUPPORTED
                    : storage == UNAVAILABLE ? CapabilityAssessment.State.UNSAFE : CapabilityAssessment.State.UNKNOWN,
                    storage == VERIFIED ? CapabilityAssessment.Reason.LOG_DIRECTORIES_HEALTHY
                    : storage == UNAVAILABLE ? CapabilityAssessment.Reason.LOG_DIRECTORY_ERROR : CapabilityAssessment.Reason.LOG_DIRECTORIES_UNAVAILABLE, true));
            capabilities.put(RACK, new CapabilityAssessment(node.rack() == null ? CapabilityAssessment.State.UNKNOWN : CapabilityAssessment.State.SUPPORTED,
                    node.rack() == null ? CapabilityAssessment.Reason.RACK_NOT_REPORTED : CapabilityAssessment.Reason.RACK_REPORTED, false));
            capabilities.put(DISK_CAPACITY, new CapabilityAssessment(usable.isEmpty() ? CapabilityAssessment.State.UNKNOWN
                    : usable.getAsLong() > 0 ? CapabilityAssessment.State.SUPPORTED : CapabilityAssessment.State.UNSAFE,
                    usable.isEmpty() ? CapabilityAssessment.Reason.USABLE_BYTES_NOT_REPORTED
                    : usable.getAsLong() > 0 ? CapabilityAssessment.Reason.POSITIVE_USABLE_BYTES : CapabilityAssessment.Reason.NO_USABLE_BYTES, false));
            capabilities.put(PLACEMENT_ADMISSION, new CapabilityAssessment(CapabilityAssessment.State.UNKNOWN,
                    CapabilityAssessment.Reason.NO_READ_ONLY_PLACEMENT_ADMISSION_API, false));
            brokers.put(node.id(), new Broker(node.id(), Optional.ofNullable(node.rack()), reachable, unfenced, storage, uncordoned, usable, capabilities));
        }

        Map<String, Topic> topics = new TreeMap<>();
        for (String name : new TreeSet<>(requested)) {
            TopicDescription description = descriptions.get(name);
            Config config = optional(configs.get(new ConfigResource(ConfigResource.Type.TOPIC, name)), deadline);
            List<Partition> partitions = description.partitions().stream().sorted(Comparator.comparingInt(p -> p.partition()))
                    .map(ElrSafetyPolicy::partition).toList();
            topics.put(name, new Topic(description.topicId().toString(), partitions, minIsr(config)));
        }
        // Detect obvious drift during collection. This is not an atomic snapshot or proof of execution readiness.
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
        boolean confirmedZooKeeper = nodes.keySet().stream().allMatch(id -> KafkaCapabilities.zooKeeper(firstBrokerConfigs.get(id)));
        // Preview retains the feature conclusion, but never a fresh execution authorization.
        ReplicationSafetyEvidence evidence = new ReplicationSafetyEvidence(OptionalLong.empty(),
                KafkaCapabilities.elr(features, confirmedZooKeeper), UNKNOWN, Map.of(), Map.of(), Map.of());
        if (preflight) {
            FeatureMetadata afterFeatures = features(admin, deadline);
            Map<ConfigResource, KafkaFuture<Config>> finalConfigs = admin.describeConfigs(resources,
                    new DescribeConfigsOptions().includeSynonyms(true).timeoutMs(ms(deadline))).values();
            Map<String, Observation> stableTopics = new TreeMap<>();
            for (String name : new TreeSet<>(requested)) {
                OptionalInt latest = minIsr(optional(finalConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, name)), deadline));
                Topic previous = topics.get(name);
                stableTopics.put(name, latest.isEmpty() || previous.minInSyncReplicas().isEmpty() ? UNKNOWN
                        : latest.equals(previous.minInSyncReplicas()) ? VERIFIED : UNAVAILABLE);
                topics.put(name, new Topic(previous.topicId(), previous.partitions(), latest));
            }
            Map<Integer, Config> lastBrokerConfigs = new TreeMap<>();
            Map<Integer, Observation> stableCordoning = new TreeMap<>();
            for (int id : nodes.keySet()) {
                Config latestConfig = optional(finalConfigs.get(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(id))), deadline);
                lastBrokerConfigs.put(id, latestConfig);
                CapabilityAssessment latestProof = KafkaCapabilities.cordoning(latestConfig, afterFeatures);
                Observation latest = cordoningObservation(latestProof);
                Broker previous = brokers.get(id);
                stableCordoning.put(id, latest == UNKNOWN || previous.uncordoned() == UNKNOWN ? UNKNOWN
                        : latest == previous.uncordoned() ? VERIFIED : UNAVAILABLE);
                Map<CapabilityAssessment.Capability, CapabilityAssessment> proofs = new EnumMap<>(previous.capabilities());
                proofs.put(CORDONING, latestProof);
                brokers.put(id, new Broker(id, previous.rack(), previous.reachable(), previous.unfenced(), previous.storage(), latest, previous.minimumUsableBytes(), proofs));
            }
            boolean zkBefore = nodes.keySet().stream().allMatch(id -> KafkaCapabilities.zooKeeper(firstBrokerConfigs.get(id)));
            boolean zkAfter = nodes.keySet().stream().allMatch(id -> KafkaCapabilities.zooKeeper(lastBrokerConfigs.get(id)));
            var mode = KafkaCapabilities.elr(afterFeatures, zkAfter);
            Observation stableFeatures = features == null || afterFeatures == null
                    || features.finalizedFeaturesEpoch().isEmpty() || afterFeatures.finalizedFeaturesEpoch().isEmpty()
                    ? UNKNOWN : features.equals(afterFeatures) ? VERIFIED : UNAVAILABLE;
            // Kafka 4.4 KRaftMetadataCache.features() returns the metadata image offset as epoch.
            // Reassignment records advance it without changing any feature. Require both complete
            // feature maps to agree and a known, non-regressing offset, never just ELR=ENABLED.
            if (mode == ReplicationSafetyEvidence.ElrMode.ENABLED
                    && KafkaCapabilities.elr(features, zkBefore) == mode
                    && features.finalizedFeaturesEpoch().isPresent() && afterFeatures.finalizedFeaturesEpoch().isPresent()
                    && features.finalizedFeaturesEpoch().get() >= 0
                    && afterFeatures.finalizedFeaturesEpoch().get() >= features.finalizedFeaturesEpoch().get()
                    && features.finalizedFeatures().equals(afterFeatures.finalizedFeatures())
                    && features.supportedFeatures().equals(afterFeatures.supportedFeatures())) stableFeatures = VERIFIED;
            // Legacy KRaft describeFeatures can sample different brokers' metadata-image offsets
            // (3.7.1 KRaftMetadataCache.features), so even a lower second epoch is not a feature
            // change. This exception requires positive ELR non-applicability on both reads,
            // identical complete feature maps and known valid epochs; ENABLED keeps its rule above.
            if (mode == ReplicationSafetyEvidence.ElrMode.NOT_APPLICABLE
                    && features != null && afterFeatures != null
                    && KafkaCapabilities.elr(features, zkBefore) == mode && zkBefore == zkAfter
                    && features.finalizedFeaturesEpoch().filter(epoch -> epoch >= 0).isPresent()
                    && afterFeatures.finalizedFeaturesEpoch().filter(epoch -> epoch >= 0).isPresent()
                    && features.finalizedFeatures().equals(afterFeatures.finalizedFeatures())
                    && features.supportedFeatures().equals(afterFeatures.supportedFeatures())) stableFeatures = VERIFIED;
            if (mode == ReplicationSafetyEvidence.ElrMode.NOT_APPLICABLE
                    && KafkaCapabilities.elr(features, zkBefore) == mode && Objects.equals(features, afterFeatures)
                    && zkBefore == zkAfter) stableFeatures = VERIFIED;
            // Neither generic placement eligibility nor required per-replica disk reserve can be proven here.
            // Deliberately do not synthesize VERIFIED admissions from Node presence or positive usableBytes.
            evidence = new ReplicationSafetyEvidence(OptionalLong.of(System.nanoTime()), mode,
                    stableFeatures, stableTopics, stableCordoning, Map.of());
        }
        return new ReplicationClusterSnapshot(clusterId, brokers, topics, finalActive.keySet(), VERIFIED, load, VERIFIED, evidence);
    }

    private static FeatureMetadata features(Admin admin, long deadline) throws InterruptedException, java.util.concurrent.TimeoutException {
        try {
            return await(admin.describeFeatures(new DescribeFeaturesOptions().timeoutMs(ms(deadline))).featureMetadata(), deadline);
        } catch (ExecutionException | RuntimeException ignored) {
            return null; // Unsupported, authorization failure or unavailable feature state is UNKNOWN.
        }
    }

    private static boolean sameTopic(TopicDescription before, TopicDescription after) {
        if (before == null || after == null || !before.topicId().equals(after.topicId())
                || !before.name().equals(after.name()) || before.partitions().size() != after.partitions().size()) {
            return false;
        }
        return before.partitions().stream().allMatch(p -> after.partitions().stream().anyMatch(f ->
                p.partition() == f.partition() && Objects.equals(p.leader(), f.leader())
                        && p.replicas().stream().map(Node::id).toList().equals(f.replicas().stream().map(Node::id).toList())
                        && new HashSet<>(p.isr().stream().map(Node::id).toList()).equals(new HashSet<>(f.isr().stream().map(Node::id).toList()))
                        && ElrSafetyPolicy.partition(p).elr().equals(ElrSafetyPolicy.partition(f).elr())
                        && ElrSafetyPolicy.partition(p).lastKnownElr().equals(ElrSafetyPolicy.partition(f).lastKnownElr())));
    }

    private static Map<Integer, Node> nodes(Collection<Node> values) {
        Map<Integer, Node> result = new TreeMap<>();
        for (Node node : values) {
            require(node != null && node.id() >= 0 && result.put(node.id(), node) == null, "RF broker metadata is invalid.");
        }
        require(!result.isEmpty(), "Resource: brokers; actual: 0; required: at least 1 live broker; state: UNKNOWN. Refresh metadata before re-planning.");
        return result;
    }

    private static Observation cordoningObservation(CapabilityAssessment assessment) {
        return switch (assessment.state()) {
            case SUPPORTED -> VERIFIED;
            case NOT_APPLICABLE -> NOT_APPLICABLE;
            case UNSAFE -> UNAVAILABLE;
            case UNKNOWN -> UNKNOWN;
        };
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
            throw new KafkaExecutionException("Cannot collect RF planning metadata.", message, KafkaExecutionException.Code.METADATA_INVALID);
        }
    }
}
