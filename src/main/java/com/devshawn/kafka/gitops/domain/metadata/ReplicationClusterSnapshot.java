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

    public enum Observation { VERIFIED, UNAVAILABLE, UNKNOWN }

    public record Broker(int id, Optional<String> rack, Observation reachable, Observation unfenced, Observation storage,
                         Observation uncordoned, OptionalLong minimumUsableBytes) {
        /** A preview candidate only. UNKNOWN fencing/capacity must not become an apply authorization. */
        public boolean isPreviewCandidate() {
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
