package com.devshawn.kafka.gitops.domain.metadata;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

/** Ephemeral read-only observations. No credentials, full Configs, hosts or log directory paths. */
public record ReplicationClusterSnapshot(String clusterId, Map<Integer, Broker> brokers,
                                         Map<String, Topic> topics, Set<TopicPartition> reassignments,
                                         Observation reassignmentState, Map<Integer, Long> replicaCounts,
                                         Observation loadState, ReplicationSafetyEvidence safety) {
    public ReplicationClusterSnapshot {
        brokers = Map.copyOf(brokers);
        topics = Map.copyOf(topics);
        reassignments = Set.copyOf(reassignments);
        replicaCounts = Map.copyOf(replicaCounts);
        java.util.Objects.requireNonNull(safety);
    }

    // Planning snapshots and existing fake providers carry no execution safety evidence.
    public ReplicationClusterSnapshot(String clusterId, Map<Integer, Broker> brokers, Map<String, Topic> topics,
                                      Set<TopicPartition> reassignments, Observation reassignmentState,
                                      Map<Integer, Long> replicaCounts, Observation loadState) {
        this(clusterId, brokers, topics, reassignments, reassignmentState, replicaCounts, loadState,
                ReplicationSafetyEvidence.unknown());
    }

    public enum Observation { VERIFIED, UNAVAILABLE, UNKNOWN, NOT_APPLICABLE }

    public record Broker(int id, Optional<String> rack, Observation reachable, Observation unfenced, Observation storage,
                         Observation uncordoned, OptionalLong minimumUsableBytes,
                         Map<CapabilityAssessment.Capability, CapabilityAssessment> capabilities) {
        public Broker { capabilities = Map.copyOf(capabilities); }
        public Broker(int id, Optional<String> rack, Observation reachable, Observation unfenced, Observation storage,
                      Observation uncordoned, OptionalLong minimumUsableBytes) {
            this(id, rack, reachable, unfenced, storage, uncordoned, minimumUsableBytes, Map.of());
        }

        public boolean cordoningEstablished() {
            var proof = capabilities.get(CapabilityAssessment.Capability.CORDONING);
            return uncordoned == Observation.VERIFIED || uncordoned == Observation.NOT_APPLICABLE
                    && proof != null && proof.state() == CapabilityAssessment.State.NOT_APPLICABLE
                    && (proof.reason() == CapabilityAssessment.Reason.METADATA_LEVEL_BELOW_CORDONING
                    || proof.reason() == CapabilityAssessment.Reason.ZOOKEEPER_MODE_CONFIRMED);
        }

        public boolean fencingEstablished() {
            var proof = capabilities.get(CapabilityAssessment.Capability.FENCING);
            return unfenced == Observation.VERIFIED || unfenced == Observation.NOT_APPLICABLE
                    && proof != null && proof.state() == CapabilityAssessment.State.NOT_APPLICABLE
                    && proof.reason() == CapabilityAssessment.Reason.ZOOKEEPER_MODE_CONFIRMED;
        }

        /** A preview candidate only. UNKNOWN fencing/capacity must not become an apply authorization. */
        public boolean isPreviewCandidate() {
            if (!capabilities.isEmpty()) {
                return reachable == Observation.VERIFIED && fencingEstablished() && storage == Observation.VERIFIED
                        && cordoningEstablished() && capabilities.values().stream().noneMatch(CapabilityAssessment::blocks);
            }
            return reachable == Observation.VERIFIED && unfenced != Observation.UNAVAILABLE && storage == Observation.VERIFIED
                    && uncordoned == Observation.VERIFIED && minimumUsableBytes.isPresent()
                    && minimumUsableBytes.getAsLong() > 0;
        }
    }

    public record Topic(String topicId, List<Partition> partitions, OptionalInt minInSyncReplicas) {
        public Topic {
            partitions = List.copyOf(partitions);
        }
    }

    public record Partition(int id, List<Integer> replicas, OptionalInt leader, Set<Integer> isr) {
        public Partition {
            replicas = List.copyOf(replicas);
            isr = Set.copyOf(isr);
        }
    }
}
