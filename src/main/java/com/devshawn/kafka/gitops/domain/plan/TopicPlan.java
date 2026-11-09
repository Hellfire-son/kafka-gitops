package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.domain.plan.serialization.TopicPlanSerializer;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;
import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = TopicPlan.Builder.class)
@JsonSerialize(using = TopicPlanSerializer.class)
public interface TopicPlan {

    String getName();

    PlanAction getAction();

    Optional<TopicDetails> getTopicDetails();

    List<TopicConfigPlan> getTopicConfigPlans();

    Optional<TopicPartitionPlan> getPartitionPlan();

    default TopicPlan toChangesOnlyPlan() {
        TopicPlan.Builder builder = new TopicPlan.Builder().setName(getName()).setAction(getAction())
                .setTopicDetails(getTopicDetails()).setPartitionPlan(getPartitionPlan());
        getTopicConfigPlans().stream().filter(it -> !it.getAction().equals(PlanAction.NO_CHANGE)).forEach(builder::addTopicConfigPlans);
        return builder.build();
    }

    class Builder extends TopicPlan_Builder {
        @Override
        @JsonSetter(value = "partitionPlan", nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        public Builder setPartitionPlan(Optional<? extends TopicPartitionPlan> value) {
            return super.setPartitionPlan(value);
        }
    }
}
