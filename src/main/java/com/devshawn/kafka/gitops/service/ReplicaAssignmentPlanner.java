package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*;
import com.devshawn.kafka.gitops.domain.plan.BrokerSnapshot;
import com.devshawn.kafka.gitops.domain.plan.PartitionReplicaAssignment;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;

import java.util.*;

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.VERIFIED;

/** Pure planner. No Kafka IO, permissions in JSON, random choices or reassignment execution. */
public final class ReplicaAssignmentPlanner {
    public Map<String, TopicReplicationPlan> plan(ReplicationClusterSnapshot snapshot, Map<String, Integer> targets,
                                                 RackAwarenessPolicy policy, boolean allowDecrease, boolean noDelete) {
        require(policy != null, "An explicit RF rack policy is required.");
        Map<String, Integer> changes = new TreeMap<>();
        new TreeMap<>(targets).forEach((name, rf) -> {
            require(rf != null && rf > 0 && rf <= Short.MAX_VALUE, "The target RF must be in 1..32767.");
            Topic topic = snapshot.topics().get(name);
            require(topic != null && !topic.partitions().isEmpty(), "RF planning requires complete topic metadata.");
            Set<Integer> ids = new HashSet<>();
            for (Partition partition : topic.partitions()) {
                require(partition.id() >= 0 && partition.id() < topic.partitions().size() && ids.add(partition.id())
                                && !partition.replicas().isEmpty()
                                && partition.replicas().stream().allMatch(id -> id != null && id >= 0)
                                && new HashSet<>(partition.replicas()).size() == partition.replicas().size(),
                        "RF planning requires unique contiguous partition IDs and unique non-negative replica IDs.");
            }
            if (topic.partitions().stream().anyMatch(p -> p.replicas().size() != rf)) {
                changes.put(name, rf);
            }
        });
        if (changes.isEmpty()) {
            return Map.of();
        }
        require(snapshot.reassignmentState() == VERIFIED, "Active reassignment state is UNKNOWN; RF planning is blocked.");
        require(snapshot.loadState() == VERIFIED, "Cluster replica count metadata is UNKNOWN; RF planning is blocked.");
        require(snapshot.reassignments().stream().noneMatch(p -> changes.containsKey(p.topic())),
                "An RF topic already has an active reassignment; wait and re-plan.");
        Map<Integer, Long> load = new HashMap<>(snapshot.replicaCounts());
        require(load.values().stream().allMatch(value -> value >= 0), "Replica count metadata is incomplete.");
        Map<Integer, Long> requiredLoad = new HashMap<>();
        changes.keySet().forEach(name -> snapshot.topics().get(name).partitions()
                .forEach(p -> p.replicas().forEach(id -> requiredLoad.merge(id, 1L, Long::sum))));
        require(requiredLoad.keySet().stream().allMatch(load::containsKey), "Replica count metadata is incomplete.");
        require(requiredLoad.entrySet().stream().allMatch(e -> load.get(e.getKey()) >= e.getValue()),
                "Replica count metadata understates the original assignments.");
        Set<Integer> eligible = new TreeSet<>();
        snapshot.brokers().forEach((id, broker) -> {
            require(id == broker.id() && id >= 0, "Broker snapshot IDs are inconsistent.");
            if (broker.isPreviewCandidate()) {
                require(load.containsKey(id), "Replica count metadata is incomplete for an eligible broker.");
                eligible.add(id);
            }
        });
        Map<String, TopicReplicationPlan> plans = new TreeMap<>();
        changes.forEach((name, rf) -> {
            Topic topic = snapshot.topics().get(name);
            require(topic.minInSyncReplicas().isPresent(), "Effective min.insync.replicas is UNKNOWN; RF planning is blocked.");
            int minIsr = topic.minInSyncReplicas().getAsInt();
            require(minIsr > 0 && rf >= minIsr, "The target RF cannot be below effective min.insync.replicas.");
            boolean decrease = topic.partitions().stream().anyMatch(p -> p.replicas().size() > rf);
            require(!decrease || !noDelete, "--no-delete forbids decreasing replication factor.");
            require(!decrease || allowDecrease, "Decreasing RF requires explicit --allow-replication-decrease for planning; apply requires independent permission.");
            Set<Integer> allowed = new TreeSet<>(eligible);
            topic.partitions().forEach(p -> allowed.addAll(p.replicas()));
            for (Partition partition : topic.partitions()) {
                require(partition.leader().isPresent() && partition.replicas().contains(partition.leader().getAsInt())
                                && partition.isr().equals(new HashSet<>(partition.replicas())),
                        "RF planning requires a current leader and the complete original ISR; unknown or degraded metadata is blocked.");
                for (int id : partition.replicas()) {
                    Broker broker = snapshot.brokers().get(id);
                    require(broker != null && broker.reachable() == VERIFIED && broker.unfenced() != Observation.UNAVAILABLE
                                    && broker.storage() == VERIFIED,
                            "An original replica broker is unavailable or UNKNOWN; RF planning is blocked.");
                }
            }
            if (policy == RackAwarenessPolicy.RACK_AWARE) {
                require(allowed.stream().allMatch(id -> snapshot.brokers().get(id).rack().isPresent()),
                        "RACK_AWARE requires complete rack metadata; choose BROKER_ONLY explicitly if appropriate.");
            }
            TopicReplicationPlan.Builder builder = new TopicReplicationPlan.Builder()
                    .setSchemaVersion(TopicReplicationPlan.SCHEMA_VERSION).setAction(PlanAction.UPDATE)
                    .setClusterId(snapshot.clusterId()).setTopicId(topic.topicId()).setPartitionCount(topic.partitions().size())
                    .setTargetReplicationFactor(rf).setAlgorithm(TopicReplicationPlan.ALGORITHM)
                    .setRackPolicy(policy).setExpectedMinInSyncReplicas(minIsr);
            for (int id : allowed) {
                builder.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(id).setRack(snapshot.brokers().get(id).rack()).build());
            }
            topic.partitions().stream().sorted(Comparator.comparingInt(Partition::id)).forEach(partition -> {
                List<Integer> previous = partition.replicas();
                List<Integer> target = new ArrayList<>(previous);
                if (rf > previous.size()) {
                    while (target.size() < rf) {
                        int selected = choose(eligible, target, snapshot, policy, load);
                        target.add(selected);
                        load.merge(selected, 1L, Long::sum);
                    }
                } else if (rf < previous.size()) {
                    List<Integer> selected = new ArrayList<>();
                    selected.add(partition.leader().getAsInt());
                    while (selected.size() < rf) {
                        selected.add(choose(previous, selected, snapshot, policy, load));
                    }
                    target = previous.stream().filter(selected::contains).toList();
                    for (int id : previous) {
                        if (!target.contains(id)) {
                            load.merge(id, -1L, Long::sum);
                        }
                    }
                }
                builder.addAssignments(new PartitionReplicaAssignment.Builder().setPartition(partition.id())
                        .addAllPreviousReplicas(previous).addAllTargetReplicas(target).build());
            });
            TopicReplicationPlan result = builder.build();
            ReplicationPlanValidation.validate(result);
            plans.put(name, result);
        });
        return Collections.unmodifiableMap(plans);
    }

    private static int choose(Collection<Integer> candidates, List<Integer> selected, ReplicationClusterSnapshot snapshot,
                              RackAwarenessPolicy policy, Map<Integer, Long> load) {
        Map<String, Long> rackCounts = new HashMap<>();
        if (policy == RackAwarenessPolicy.RACK_AWARE) {
            selected.forEach(id -> rackCounts.merge(snapshot.brokers().get(id).rack().orElseThrow(), 1L, Long::sum));
        }
        Comparator<Integer> comparator = Comparator.comparingLong((Integer id) -> policy == RackAwarenessPolicy.RACK_AWARE
                        ? rackCounts.getOrDefault(snapshot.brokers().get(id).rack().orElseThrow(), 0L) : 0L)
                .thenComparingLong(id -> load.getOrDefault(id, Long.MAX_VALUE)).thenComparingInt(Integer::intValue);
        return candidates.stream().filter(id -> !selected.contains(id)).min(comparator)
                .orElseThrow(() -> new ValidationException("Too few verified eligible brokers for the target RF; UNKNOWN, unavailable or cordoned brokers cannot be used."));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ValidationException(message);
        }
    }
}
