// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.exception.ValidationException;

public final class PlanValidation {
    private PlanValidation() {
    }

    public static void validate(DesiredPlan plan) {
        require(plan != null, "A plan must be an object.");
        java.util.Set<String> topics = new java.util.HashSet<>();
        for (TopicPlan topic : plan.getTopicPlans()) {
            require(topic != null, "A topic plan must be an object.");
            require(topics.add(topic.getName()), "Duplicate topic operations are unsupported.");
            ReplicationPlanValidation.validate(topic);
            topic.getTopicDetails().ifPresent(details -> require(details.getPartitions() > 0,
                    "The partition count must be a positive integer."));
            topic.getTopicDetails().flatMap(details -> details.getReplication()).ifPresent(rf ->
                    require(rf >= 1 && rf <= Short.MAX_VALUE, "Replication must be in 1..32767."));
            topic.getPartitionPlan().ifPresent(partitions -> {
                require(topic.getAction() == PlanAction.UPDATE, "Partition changes require a topic UPDATE.");
                require(partitions.getAction() == PlanAction.UPDATE, "Only partition UPDATE is supported.");
                require(partitions.getPreviousCount() > 0 && partitions.getTargetCount() > partitions.getPreviousCount(),
                        "A partition UPDATE requires a positive previous count and a larger target count.");
                topic.getTopicDetails().ifPresent(details -> require(details.getPartitions() == partitions.getTargetCount(),
                        "The partition target must match topicDetails.partitions."));
            });
            if (topic.getAction() == PlanAction.ADD) {
                require(topic.getTopicDetails().isPresent(), "A topic ADD requires topicDetails.");
                require(topic.getTopicDetails().get().getReplication().isPresent(), "A topic ADD requires replication.");
                require(topic.getTopicConfigPlans().isEmpty(), "A topic ADD uses topicDetails.configs, not config operations.");
            } else if (topic.getAction() == PlanAction.UPDATE) {
                require(topic.getPartitionPlan().isPresent()
                                || topic.getReplicationPlan().isPresent()
                                || topic.getTopicConfigPlans().stream().anyMatch(config -> config.getAction() != PlanAction.NO_CHANGE),
                        "A topic UPDATE requires configuration, partition or replication changes.");
            } else {
                require(topic.getTopicConfigPlans().stream().allMatch(config -> config.getAction() == PlanAction.NO_CHANGE),
                        "Configuration changes require a topic UPDATE.");
            }
            for (TopicConfigPlan config : topic.getTopicConfigPlans()) {
                if (config.getAction() == PlanAction.ADD || config.getAction() == PlanAction.UPDATE) {
                    require(config.getValue().isPresent(), "A configuration ADD or UPDATE requires a value.");
                }
            }
        }
        plan.getAclPlans().forEach(acl -> require(acl.getAction() != PlanAction.UPDATE, "ACL UPDATE is unsupported."));
    }

    public static void validateReplicationPlans(DesiredPlan plan) {
        plan.getTopicPlans().forEach(ReplicationPlanValidation::validate);
    }

    public static void validateForApply(DesiredPlan plan) {
        validate(plan);
        if (plan.getTopicPlans().stream().anyMatch(topic -> topic.getReplicationPlan().isPresent())) {
            throw new ValidationException("Replication plan application is unavailable in Stage 6B1. No topic operations were started.");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
