package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;


public final class TopicReplicationPlanSerializer extends StdSerializer<TopicReplicationPlan> {
    public TopicReplicationPlanSerializer() {
        super(TopicReplicationPlan.class);
    }

    @Override
    public void serialize(TopicReplicationPlan plan, JsonGenerator generator, SerializationContext provider) {
        try {
            ReplicationPlanValidation.validate(plan);
        } catch (ReplicationPlanValidation.InvalidReplicationPlanException ex) {
            throw DatabindException.from(generator, ex.getMessage());
        }
        generator.writeStartObject();
        generator.writeNumberProperty("schemaVersion", plan.getSchemaVersion());
        generator.writePOJOProperty("action", plan.getAction());
        generator.writeStringProperty("clusterId", plan.getClusterId());
        generator.writeStringProperty("topicId", plan.getTopicId());
        generator.writeNumberProperty("partitionCount", plan.getPartitionCount());
        generator.writeNumberProperty("targetReplicationFactor", plan.getTargetReplicationFactor());
        generator.writeStringProperty("algorithm", plan.getAlgorithm());
        generator.writePOJOProperty("rackPolicy", plan.getRackPolicy());
        generator.writeNumberProperty("expectedMinInSyncReplicas", plan.getExpectedMinInSyncReplicas());
        generator.writePOJOProperty("brokerSnapshot", plan.getBrokerSnapshot());
        generator.writePOJOProperty("assignments", plan.getAssignments());
        generator.writeEndObject();
    }
}
