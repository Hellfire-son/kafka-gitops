// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.BrokerSnapshot;
import com.devshawn.kafka.gitops.domain.plan.PartitionReplicaAssignment;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import org.apache.kafka.common.Uuid;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Structural checks only. A serialized snapshot is not proof of live Kafka state or permission. */
public final class ReplicationPlanValidation {
    private ReplicationPlanValidation() {
    }

    public static void validate(TopicPlan topic) {
        topic.getReplicationPlan().ifPresent(replication -> {
            require(topic.getName().matches("[a-zA-Z0-9._-]{1,249}")
                            && !topic.getName().equals(".") && !topic.getName().equals(".."),
                    "A replicationPlan requires a valid Kafka topic name.");
            require(topic.getAction() == PlanAction.UPDATE, "replicationPlan requires a topic UPDATE.");
            require(topic.getPartitionPlan().isEmpty(), "partitionPlan and replicationPlan cannot be combined for one topic.");
            validate(replication);
            topic.getTopicDetails().ifPresent(details -> {
                require(details.getPartitions() == replication.getPartitionCount(),
                        "topicDetails.partitions must match replicationPlan.partitionCount.");
                details.getReplication().ifPresent(rf -> require(rf == replication.getTargetReplicationFactor(),
                        "topicDetails.replication must match replicationPlan.targetReplicationFactor."));
            });
        });
    }

    public static void validate(TopicReplicationPlan replication) {
        require(replication != null, "replicationPlan must be an object.");
        require(replication.getSchemaVersion() == TopicReplicationPlan.SCHEMA_VERSION,
                "Unsupported replicationPlan schemaVersion; only version 1 is supported.");
        require(replication.getAction() == PlanAction.UPDATE, "Only replicationPlan UPDATE is supported.");
        require(TopicReplicationPlan.ALGORITHM.equals(replication.getAlgorithm()), "Unsupported replicationPlan algorithm.");
        require(validUuid(replication.getClusterId()), "replicationPlan.clusterId must be a canonical non-reserved Kafka UUID.");
        require(validUuid(replication.getTopicId()), "replicationPlan.topicId must be a canonical non-reserved Kafka UUID.");
        require(replication.getPartitionCount() > 0, "replicationPlan.partitionCount must be positive.");
        require(replication.getTargetReplicationFactor() > 0 && replication.getTargetReplicationFactor() <= Short.MAX_VALUE,
                "replicationPlan.targetReplicationFactor must be positive and at most 32767.");
        require(replication.getExpectedMinInSyncReplicas() > 0
                        && replication.getExpectedMinInSyncReplicas() <= replication.getTargetReplicationFactor(),
                "replicationPlan.expectedMinInSyncReplicas must be positive and cannot exceed the target RF.");

        Set<Integer> brokerIds = new HashSet<>();
        for (BrokerSnapshot broker : replication.getBrokerSnapshot()) {
            require(broker != null && broker.getId() >= 0, "brokerSnapshot requires non-negative broker IDs.");
            require(brokerIds.add(broker.getId()), "brokerSnapshot contains duplicate broker IDs.");
            broker.getRack().ifPresent(rack -> require(!rack.isBlank() && rack.length() <= 255
                            && rack.codePoints().noneMatch(Character::isISOControl),
                    "brokerSnapshot.rack must be a non-blank label without control characters, at most 255 characters."));
            require(replication.getRackPolicy() != RackAwarenessPolicy.RACK_AWARE || broker.getRack().isPresent(),
                    "RACK_AWARE requires a rack label for every broker in brokerSnapshot.");
        }
        require(brokerIds.size() >= replication.getTargetReplicationFactor(), "brokerSnapshot has too few distinct brokers for the target RF.");
        require(replication.getAssignments().size() == replication.getPartitionCount(),
                "replicationPlan.assignments must cover every partition exactly once.");

        Set<Integer> partitions = new HashSet<>();
        boolean changed = false;
        for (PartitionReplicaAssignment assignment : replication.getAssignments()) {
            require(assignment != null && assignment.getPartition() >= 0
                            && assignment.getPartition() < replication.getPartitionCount(),
                    "assignment.partition is outside replicationPlan.partitionCount.");
            require(partitions.add(assignment.getPartition()), "replicationPlan.assignments contains duplicate partition IDs.");
            List<Integer> previous = assignment.getPreviousReplicas();
            List<Integer> target = assignment.getTargetReplicas();
            validateReplicas(previous, brokerIds);
            validateReplicas(target, brokerIds);
            require(target.size() == replication.getTargetReplicationFactor(), "Every targetReplicas list must have the target RF size.");
            if (target.size() == previous.size()) {
                require(target.equals(previous), "An unchanged RF must not change replica IDs or their order.");
            } else if (target.size() > previous.size()) {
                require(target.subList(0, previous.size()).equals(previous),
                        "An RF increase must preserve the ordered previousReplicas prefix.");
                changed = true;
            } else {
                require(isSubsequence(target, previous), "An RF decrease must retain an ordered subset of previousReplicas.");
                changed = true;
            }
        }
        require(changed, "replicationPlan must contain at least one actual RF change.");
    }

    private static void validateReplicas(List<Integer> replicas, Set<Integer> brokers) {
        require(!replicas.isEmpty(), "Replica assignments must not be empty.");
        Set<Integer> unique = new HashSet<>();
        for (Integer broker : replicas) {
            require(broker != null && broker >= 0 && brokers.contains(broker),
                    "Replica assignments must reference non-negative broker IDs from brokerSnapshot.");
            require(unique.add(broker), "Replica assignments contain duplicate broker IDs.");
        }
    }

    private static boolean isSubsequence(List<Integer> subset, List<Integer> previous) {
        int matched = 0;
        for (Integer broker : previous) {
            if (matched < subset.size() && broker.equals(subset.get(matched))) {
                matched++;
            }
        }
        return matched == subset.size();
    }

    private static boolean validUuid(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]{22}")) {
            return false;
        }
        try {
            Uuid uuid = Uuid.fromString(value);
            return !Uuid.RESERVED.contains(uuid) && uuid.toString().equals(value);
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new InvalidReplicationPlanException(message);
        }
    }

    /** Messages are fixed diagnostics and never include untrusted config, identifier or rack values. */
    public static final class InvalidReplicationPlanException extends IllegalArgumentException {
        private InvalidReplicationPlanException(String message) {
            super(message);
        }
    }
}
