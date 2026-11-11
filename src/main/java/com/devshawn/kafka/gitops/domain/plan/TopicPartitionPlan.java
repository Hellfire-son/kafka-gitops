package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.enums.PlanAction;
import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = TopicPartitionPlan.Builder.class)
@JsonDeserialize(builder = TopicPartitionPlan.Builder.class)
public interface TopicPartitionPlan {
    PlanAction getAction();

    int getPreviousCount();

    int getTargetCount();

    class Builder extends TopicPartitionPlan_Builder {
    }
}
