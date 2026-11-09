package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.domain.plan.serialization.TopicConfigPlanSerializer;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = TopicConfigPlan.Builder.class)
@JsonSerialize(using = TopicConfigPlanSerializer.class)
public interface TopicConfigPlan {

    String getKey();

    Optional<String> getValue();

    Optional<String> getPreviousValue();

    boolean isSensitive();

    PlanAction getAction();

    class Builder extends TopicConfigPlan_Builder {
        public Builder() {
            setSensitive(false);
        }
    }
}
