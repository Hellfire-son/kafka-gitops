package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence;
import com.devshawn.kafka.gitops.domain.plan.*;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;

import java.time.Duration;
import java.util.*;

import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.*;
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Reason.*;
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*;

/** Pure policy. READY is a preflight decision, never evidence that an RF operation has executed. */
public final class ReplicationSafetyPolicy {
    private static final Set<String> INCOMPATIBLE_CONFIGS = Set.of("min.insync.replicas", "unclean.leader.election.enable",
            "leader.replication.throttled.replicas", "follower.replication.throttled.replicas");

    public record Permissions(boolean manageReplication, boolean allowDecrease, boolean noDelete) { }

    public record Constraints(boolean partitionConflict, boolean configConflict, long readStartedNanos,
                              long nowNanos, Duration maxSnapshotAge) {
        public Constraints {
            if (maxSnapshotAge == null || maxSnapshotAge.isNegative() || maxSnapshotAge.isZero()
                    || maxSnapshotAge.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("RF snapshot age must be positive and at most one minute.");
            }
        }

        public static Constraints forTopic(TopicPlan topic, long started, long now) {
            boolean conflict = topic.getTopicConfigPlans().stream().anyMatch(c -> c.getAction() != PlanAction.NO_CHANGE
                    && INCOMPATIBLE_CONFIGS.contains(c.getKey()))
                    || topic.getTopicDetails().stream().anyMatch(d -> d.getConfigs().keySet().stream().anyMatch(INCOMPATIBLE_CONFIGS::contains));
            return new Constraints(topic.getPartitionPlan().isPresent(), conflict, started, now, Duration.ofSeconds(30));
        }
    }

    public List<Finding> permissions(TopicReplicationPlan plan, Permissions permissions) {
        List<Finding> findings = new ArrayList<>();
        if (permissions.allowDecrease() && !permissions.manageReplication()) add(findings, INVALID_PERMISSION_COMBINATION, Condition.INVALID);
        if (!permissions.manageReplication()) add(findings, MANAGE_REPLICATION_REQUIRED, Condition.INVALID);
        if (plan.requiresDecreaseApproval()) {
            if (!permissions.allowDecrease()) add(findings, DECREASE_PERMISSION_REQUIRED, Condition.INVALID);
            if (permissions.noDelete()) add(findings, NO_DELETE, Condition.INVALID);
        }
        return List.copyOf(findings);
    }

    public ReplicationSafetyResult blockedBeforeMetadata(TopicReplicationPlan plan, List<Finding> reasons) {
        List<Finding> blocked = reasons.isEmpty() ? List.of(Finding.global(PREFLIGHT_NOT_PERFORMED, Condition.UNKNOWN)) : reasons;
        return decisions(plan, blocked, Map.of(), List.of(Warning.EXECUTION_UNAVAILABLE));
    }

    public ReplicationSafetyResult evaluate(String topicName, TopicReplicationPlan plan, ReplicationClusterSnapshot current,
                                             Permissions permissions, Constraints constraints) {
        return evaluateForExecution(topicName, plan, current, permissions, constraints, Set.of());
    }

    /** Already completed partitions are checked against the actual target, never against fabricated original metadata.
     * All freshness, permission, broker, admission, capacity and feature checks remain mandatory before another batch.
     * Active reassignments are still forbidden here: the executor must finish observation before sending another batch.
     */
    public ReplicationSafetyResult evaluateForExecution(String topicName, TopicReplicationPlan plan, ReplicationClusterSnapshot current,
                                                       Permissions permissions, Constraints constraints, Set<Integer> completedPartitions) {
        List<Finding> global = new ArrayList<>();
        List<Warning> warnings = new ArrayList<>(List.of(Warning.NON_ATOMIC_SNAPSHOT,
                Warning.LEADER_NOT_PINNED_IN_SCHEMA_V1, Warning.EXECUTION_UNAVAILABLE));
        try {
            ReplicationPlanValidation.validate(plan);
            if (completedPartitions == null || completedPartitions.stream().anyMatch(id -> id == null || id < 0 || id >= plan.getPartitionCount())) {
                throw new IllegalArgumentException("Invalid completed partition scope.");
            }
        } catch (IllegalArgumentException ex) {
            add(global, STRUCTURE_INVALID, Condition.INVALID);
            return new ReplicationSafetyResult(global, warnings, List.of());
        }
        if (plan.getRackPolicy() == RackAwarenessPolicy.BROKER_ONLY) warnings.add(Warning.BROKER_ONLY);
        global.addAll(permissions(plan, permissions));
        if (constraints.partitionConflict()) add(global, PARTITION_CONFLICT, Condition.INVALID);
        if (constraints.configConflict()) add(global, CONFIG_CONFLICT, Condition.INVALID);
        if (current == null) {
            add(global, METADATA_UNAVAILABLE, Condition.UNKNOWN);
            return decisions(plan, global, Map.of(), warnings);
        }
        ReplicationSafetyEvidence evidence = current.safety();
        if (evidence.completedAtNanos().isEmpty()
                || constraints.nowNanos() - constraints.readStartedNanos() < 0
                || constraints.nowNanos() - constraints.readStartedNanos() > constraints.maxSnapshotAge().toNanos()
                || constraints.nowNanos() - evidence.completedAtNanos().getAsLong() < 0
                || constraints.nowNanos() - evidence.completedAtNanos().getAsLong() > constraints.maxSnapshotAge().toNanos()
                || evidence.completedAtNanos().getAsLong() - constraints.readStartedNanos() < 0) {
            add(global, SNAPSHOT_NOT_FRESH, Condition.UNKNOWN);
        }
        if (!Objects.equals(plan.getClusterId(), current.clusterId())) add(global, CLUSTER_ID_CHANGED, Condition.INVALID);
        if (current.reassignmentState() != VERIFIED) add(global, REASSIGNMENT_UNKNOWN, Condition.UNKNOWN);
        if (current.reassignments().stream().anyMatch(p -> p.topic().equals(topicName))) add(global, ACTIVE_REASSIGNMENT, Condition.INVALID);
        if (evidence.elrMode() == ReplicationSafetyEvidence.ElrMode.UNKNOWN) add(global, ELR_MODE_UNKNOWN, Condition.UNKNOWN);
        if (evidence.elrMode() == ReplicationSafetyEvidence.ElrMode.ENABLED) add(global, ELR_UNSUPPORTED, Condition.UNKNOWN);
        observation(global, evidence.featuresStable(), FEATURES_CHANGED, ELR_MODE_UNKNOWN);
        observation(global, evidence.configsStable().getOrDefault(topicName, UNKNOWN), CONFIG_CHANGED, CONFIG_STATE_UNKNOWN);
        Topic topic = current.topics().get(topicName);
        if (topic == null) {
            add(global, TOPIC_NOT_FOUND, Condition.INVALID);
            return decisions(plan, global, Map.of(), warnings);
        }
        if (!Objects.equals(plan.getTopicId(), topic.topicId())) add(global, TOPIC_ID_CHANGED, Condition.INVALID);
        Set<Integer> ids = new HashSet<>();
        if (topic.partitions().size() != plan.getPartitionCount() || topic.partitions().stream().anyMatch(p ->
                p.id() < 0 || p.id() >= plan.getPartitionCount() || !ids.add(p.id()))) {
            add(global, PARTITION_COUNT_CHANGED, Condition.INVALID);
        }
        if (topic.minInSyncReplicas().isEmpty() || topic.minInSyncReplicas().getAsInt() <= 0) add(global, MIN_ISR_UNKNOWN, Condition.UNKNOWN);
        else {
            int minIsr = topic.minInSyncReplicas().getAsInt();
            if (minIsr != plan.getExpectedMinInSyncReplicas()) add(global, MIN_ISR_CHANGED, Condition.INVALID);
            if (plan.getTargetReplicationFactor() < minIsr) add(global, TARGET_BELOW_MIN_ISR, Condition.INVALID);
        }
        Set<Integer> plannedBrokers = new HashSet<>();
        for (BrokerSnapshot broker : plan.getBrokerSnapshot()) {
            plannedBrokers.add(broker.getId());
            Broker actual = current.brokers().get(broker.getId());
            if (actual == null || actual.id() != broker.getId() || !broker.getRack().equals(actual.rack())) add(global, TOPOLOGY_CHANGED, Condition.INVALID);
            if (plan.getRackPolicy() == RackAwarenessPolicy.RACK_AWARE && (actual == null || actual.rack().isEmpty())) add(global, RACK_METADATA_UNKNOWN, Condition.UNKNOWN);
        }
        if (current.brokers().values().stream().anyMatch(b -> b.isPreviewCandidate() && !plannedBrokers.contains(b.id()))) {
            add(global, TOPOLOGY_CHANGED, Condition.INVALID);
        }
        Map<Integer, List<Finding>> partitions = new TreeMap<>();
        for (PartitionReplicaAssignment assignment : plan.getAssignments()) {
            List<Finding> issues = new ArrayList<>();
            int partitionId = assignment.getPartition();
            Partition actual = topic.partitions().stream().filter(p -> p.id() == partitionId).findFirst().orElse(null);
            if (actual == null) {
                issues.add(finding(PARTITION_COUNT_CHANGED, Condition.INVALID, partitionId, null));
            } else {
                List<Integer> expected = completedPartitions.contains(partitionId) ? assignment.getTargetReplicas() : assignment.getPreviousReplicas();
                if (!expected.equals(actual.replicas())) issues.add(finding(PREVIOUS_REPLICAS_CHANGED, Condition.INVALID, partitionId, null));
                if (actual.leader().isEmpty()) issues.add(finding(LEADER_UNKNOWN, Condition.UNKNOWN, partitionId, null));
                else if (!actual.replicas().contains(actual.leader().getAsInt()) || !actual.isr().contains(actual.leader().getAsInt())) issues.add(finding(LEADER_NOT_IN_ISR, Condition.INVALID, partitionId, null));
                if (!actual.isr().equals(new HashSet<>(actual.replicas()))) issues.add(finding(ISR_INCOMPLETE, Condition.INVALID, partitionId, null));
                if (assignment.getTargetReplicas().size() < assignment.getPreviousReplicas().size()
                        && !actual.isr().containsAll(assignment.getTargetReplicas())) issues.add(finding(TARGET_NOT_IN_ISR, Condition.INVALID, partitionId, null));
                if (assignment.getTargetReplicas().size() < assignment.getPreviousReplicas().size() && actual.leader().isPresent()
                        && !assignment.getTargetReplicas().contains(actual.leader().getAsInt())) issues.add(finding(LEADER_NOT_RETAINED, Condition.INVALID, partitionId, null));
            }
            Set<Integer> all = new TreeSet<>(assignment.getPreviousReplicas());
            all.addAll(assignment.getTargetReplicas());
            for (int id : all) {
                Broker broker = current.brokers().get(id);
                if (broker == null) { issues.add(finding(TOPOLOGY_CHANGED, Condition.INVALID, partitionId, id)); continue; }
                check(issues, broker.reachable(), BROKER_OFFLINE, BROKER_HEALTH_UNKNOWN, partitionId, id);
                check(issues, broker.unfenced(), BROKER_FENCED, BROKER_FENCING_UNKNOWN, partitionId, id);
                check(issues, broker.storage(), STORAGE_UNAVAILABLE, STORAGE_UNKNOWN, partitionId, id);
                if (assignment.getTargetReplicas().contains(id)) {
                    check(issues, broker.uncordoned(), BROKER_CORDONED, CORDONING_UNKNOWN, partitionId, id);
                    check(issues, evidence.cordoningStable().getOrDefault(id, UNKNOWN), CORDONING_CHANGED, CORDONING_UNKNOWN, partitionId, id);
                    var admission = evidence.admissions().getOrDefault(id, ReplicationSafetyEvidence.Admission.unknown());
                    check(issues, admission.placementAllowed(), PLACEMENT_FORBIDDEN, PLACEMENT_UNKNOWN, partitionId, id);
                    if (!assignment.getPreviousReplicas().contains(id)) {
                        if (broker.minimumUsableBytes().isEmpty()) issues.add(finding(DISK_CAPACITY_UNKNOWN, Condition.UNKNOWN, partitionId, id));
                        else if (broker.minimumUsableBytes().getAsLong() <= 0) issues.add(finding(DISK_INSUFFICIENT, Condition.INVALID, partitionId, id));
                        check(issues, admission.diskCapacitySufficient(), DISK_INSUFFICIENT, DISK_CAPACITY_UNKNOWN, partitionId, id);
                    }
                }
            }
            if (plan.getRackPolicy() == RackAwarenessPolicy.RACK_AWARE) rackCheck(assignment, current, plannedBrokers, issues);
            partitions.put(partitionId, issues);
        }
        return decisions(plan, global, partitions, warnings);
    }

    private static void rackCheck(PartitionReplicaAssignment assignment, ReplicationClusterSnapshot current,
                                  Set<Integer> plannedBrokers, List<Finding> issues) {
        Map<String, Integer> counts = new HashMap<>();
        Set<Integer> seen = new HashSet<>();
        // Existing assignments are preserved; rack validation only constrains newly appended replicas.
        for (int id : assignment.getTargetReplicas()) {
            Broker broker = current.brokers().get(id);
            if (broker == null || broker.rack().isEmpty()) continue;
            String rack = broker.rack().get();
            if (!assignment.getPreviousReplicas().contains(id)) {
                int chosen = counts.getOrDefault(rack, 0);
                boolean better = current.brokers().values().stream().filter(Broker::isPreviewCandidate)
                        .filter(b -> plannedBrokers.contains(b.id()) && !seen.contains(b.id()) && b.id() != id && b.rack().isPresent())
                        .anyMatch(b -> counts.getOrDefault(b.rack().get(), 0) < chosen);
                if (better) issues.add(finding(RACK_POLICY_VIOLATION, Condition.INVALID, assignment.getPartition(), id));
            }
            counts.merge(rack, 1, Integer::sum);
            seen.add(id);
        }
        if (assignment.getTargetReplicas().size() < assignment.getPreviousReplicas().size()) {
            long availableRacks = assignment.getPreviousReplicas().stream().map(current.brokers()::get).filter(Objects::nonNull)
                    .flatMap(b -> b.rack().stream()).distinct().count();
            if (counts.size() < Math.min(assignment.getTargetReplicas().size(), availableRacks)) {
                issues.add(finding(RACK_POLICY_VIOLATION, Condition.INVALID, assignment.getPartition(), null));
            }
        }
    }

    private static ReplicationSafetyResult decisions(TopicReplicationPlan plan, List<Finding> global,
                                                    Map<Integer, List<Finding>> perPartition, List<Warning> warnings) {
        List<Finding> all = new ArrayList<>(global);
        perPartition.values().forEach(all::addAll);
        List<PartitionDecision> operations = plan.getAssignments().stream().map(a -> {
            List<Finding> issues = new ArrayList<>(all); // One failing partition blocks the whole RF operation.
            Operation operation = a.getTargetReplicas().size() > a.getPreviousReplicas().size() ? Operation.INCREASE
                    : a.getTargetReplicas().size() < a.getPreviousReplicas().size() ? Operation.DECREASE : Operation.UNCHANGED;
            return new PartitionDecision(a.getPartition(), operation, issues);
        }).toList();
        return new ReplicationSafetyResult(all, warnings, operations);
    }

    private static Finding finding(Reason code, Condition condition, Integer partition, Integer broker) {
        return new Finding(code, condition, partition == null ? OptionalInt.empty() : OptionalInt.of(partition),
                broker == null ? OptionalInt.empty() : OptionalInt.of(broker));
    }
    private static void add(List<Finding> into, Reason code, Condition condition) { into.add(Finding.global(code, condition)); }
    private static void observation(List<Finding> into, Observation value, Reason invalid, Reason unknown) {
        if (value != VERIFIED) add(into, value == UNAVAILABLE ? invalid : unknown, value == UNAVAILABLE ? Condition.INVALID : Condition.UNKNOWN);
    }
    private static void check(List<Finding> into, Observation value, Reason invalid, Reason unknown, int partition, int broker) {
        if (value != VERIFIED) into.add(finding(value == UNAVAILABLE ? invalid : unknown,
                value == UNAVAILABLE ? Condition.INVALID : Condition.UNKNOWN, partition, broker));
    }
}
