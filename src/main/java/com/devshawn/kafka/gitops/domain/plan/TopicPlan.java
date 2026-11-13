// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.domain.plan.serialization.TopicPlanSerializer;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;
import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = TopicPlan.Builder.class)
@JsonDeserialize(builder = TopicPlan.Builder.class)
@JsonSerialize(using = TopicPlanSerializer.class)
public interface TopicPlan {

    String getName();

    PlanAction getAction();

    Optional<TopicDetails> getTopicDetails();

    List<TopicConfigPlan> getTopicConfigPlans();

    Optional<TopicPartitionPlan> getPartitionPlan();

    Optional<TopicReplicationPlan> getReplicationPlan();

    default TopicPlan toChangesOnlyPlan() {
        ReplicationPlanValidation.validate(this);
        TopicPlan.Builder builder = new TopicPlan.Builder().setName(getName()).setAction(getAction())
                .setTopicDetails(getTopicDetails()).setPartitionPlan(getPartitionPlan()).setReplicationPlan(getReplicationPlan());
        getTopicConfigPlans().stream().filter(it -> !it.getAction().equals(PlanAction.NO_CHANGE)).forEach(builder::addTopicConfigPlans);
        return builder.build();
    }

    class Builder extends TopicPlan_Builder {
        @Override
        @JsonSetter(value = "replicationPlan", nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        public Builder setReplicationPlan(Optional<? extends TopicReplicationPlan> value) {
            return super.setReplicationPlan(value);
        }

        @Override
        @JsonSetter(value = "partitionPlan", nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
        public Builder setPartitionPlan(Optional<? extends TopicPartitionPlan> value) {
            return super.setPartitionPlan(value);
        }
    }
}
