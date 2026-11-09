package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;

public final class PlanValidation {
    private PlanValidation() {
    }

    public static void validate(DesiredPlan plan) {
        require(plan != null, "A plan must be an object.");
        for (TopicPlan topic : plan.getTopicPlans()) {
            if (topic.getAction() == PlanAction.ADD) {
                require(topic.getTopicDetails().isPresent(), "A topic ADD requires topicDetails.");
                require(topic.getTopicDetails().get().getReplication().isPresent(), "A topic ADD requires replication.");
                require(topic.getTopicConfigPlans().isEmpty(), "A topic ADD uses topicDetails.configs, not config operations.");
            } else if (topic.getAction() == PlanAction.UPDATE) {
                require(topic.getTopicConfigPlans().stream().anyMatch(config -> config.getAction() != PlanAction.NO_CHANGE),
                        "A topic UPDATE requires configuration changes.");
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

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
