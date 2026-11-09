package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.enums.PlanAction;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@JsonDeserialize(builder = TopicPartitionPlan.Builder.class)
public interface TopicPartitionPlan {
    PlanAction getAction();

    int getPreviousCount();

    int getTargetCount();

    class Builder extends TopicPartitionPlan_Builder {
    }
}
