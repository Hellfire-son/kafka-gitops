package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.domain.plan.serialization.TopicReplicationPlanSerializer;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;

@FreeBuilder
@JsonDeserialize(builder = TopicReplicationPlan.Builder.class)
@JsonSerialize(using = TopicReplicationPlanSerializer.class)
public interface TopicReplicationPlan {
    int SCHEMA_VERSION = 1;
    String ALGORITHM = "preserve-rack-load-v1";

    int getSchemaVersion();

    PlanAction getAction();

    String getClusterId();

    String getTopicId();

    int getPartitionCount();

    int getTargetReplicationFactor();

    String getAlgorithm();

    RackAwarenessPolicy getRackPolicy();

    int getExpectedMinInSyncReplicas();

    List<BrokerSnapshot> getBrokerSnapshot();

    List<PartitionReplicaAssignment> getAssignments();

    // Derived risk metadata, never an authorization and never a JSON input property.
    default boolean requiresDecreaseApproval() {
        return getAssignments().stream().anyMatch(a -> a.getTargetReplicas().size() < a.getPreviousReplicas().size());
    }

    default boolean hasIncreases() {
        return getAssignments().stream().anyMatch(a -> a.getTargetReplicas().size() > a.getPreviousReplicas().size());
    }

    class Builder extends TopicReplicationPlan_Builder {
    }
}
