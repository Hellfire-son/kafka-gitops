package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public final class TopicReplicationPlanSerializer extends StdSerializer<TopicReplicationPlan> {
    public TopicReplicationPlanSerializer() {
        super(TopicReplicationPlan.class);
    }

    @Override
    public void serialize(TopicReplicationPlan plan, JsonGenerator generator, SerializerProvider provider) throws IOException {
        try {
            ReplicationPlanValidation.validate(plan);
        } catch (ReplicationPlanValidation.InvalidReplicationPlanException ex) {
            throw JsonMappingException.from(generator, ex.getMessage());
        }
        generator.writeStartObject();
        generator.writeNumberField("schemaVersion", plan.getSchemaVersion());
        generator.writeObjectField("action", plan.getAction());
        generator.writeStringField("clusterId", plan.getClusterId());
        generator.writeStringField("topicId", plan.getTopicId());
        generator.writeNumberField("partitionCount", plan.getPartitionCount());
        generator.writeNumberField("targetReplicationFactor", plan.getTargetReplicationFactor());
        generator.writeStringField("algorithm", plan.getAlgorithm());
        generator.writeObjectField("rackPolicy", plan.getRackPolicy());
        generator.writeNumberField("expectedMinInSyncReplicas", plan.getExpectedMinInSyncReplicas());
        generator.writeObjectField("brokerSnapshot", plan.getBrokerSnapshot());
        generator.writeObjectField("assignments", plan.getAssignments());
        generator.writeEndObject();
    }
}
