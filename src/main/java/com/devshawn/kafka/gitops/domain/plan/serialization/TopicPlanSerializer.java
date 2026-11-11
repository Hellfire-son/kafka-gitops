package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;


public final class TopicPlanSerializer extends StdSerializer<TopicPlan> {
    public TopicPlanSerializer() {
        super(TopicPlan.class);
    }

    @Override
    public void serialize(TopicPlan topic, JsonGenerator generator, SerializationContext provider) {
        // Desired values also occur in topicDetails.configs, including for newly created topics.
        if (TopicConfigSecurity.hasSensitiveValues(topic)) {
            throw DatabindException.from(generator, "Confidential configuration values cannot be exported. Apply directly from the state file.");
        }
        try {
            ReplicationPlanValidation.validate(topic);
        } catch (ReplicationPlanValidation.InvalidReplicationPlanException ex) {
            throw DatabindException.from(generator, ex.getMessage());
        }
        generator.writeStartObject();
        generator.writeStringProperty("name", topic.getName());
        generator.writePOJOProperty("action", topic.getAction());
        generator.writePOJOProperty("topicDetails", topic.getTopicDetails().orElse(null));
        generator.writePOJOProperty("topicConfigPlans", topic.getTopicConfigPlans());
        if (topic.getPartitionPlan().isPresent()) {
            generator.writePOJOProperty("partitionPlan", topic.getPartitionPlan().get());
        }
        if (topic.getReplicationPlan().isPresent()) {
            generator.writePOJOProperty("replicationPlan", topic.getReplicationPlan().get());
        }
        generator.writeEndObject();
    }
}
