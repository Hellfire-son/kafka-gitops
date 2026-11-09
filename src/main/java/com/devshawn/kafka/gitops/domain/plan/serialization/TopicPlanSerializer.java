package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public final class TopicPlanSerializer extends StdSerializer<TopicPlan> {
    public TopicPlanSerializer() {
        super(TopicPlan.class);
    }

    @Override
    public void serialize(TopicPlan topic, JsonGenerator generator, SerializerProvider provider) throws IOException {
        // Desired values also occur in topicDetails.configs, including for newly created topics.
        if (TopicConfigSecurity.hasSensitiveValues(topic)) {
            throw JsonMappingException.from(generator, "Confidential configuration values cannot be exported. Apply directly from the state file.");
        }
        generator.writeStartObject();
        generator.writeStringField("name", topic.getName());
        generator.writeObjectField("action", topic.getAction());
        generator.writeObjectField("topicDetails", topic.getTopicDetails().orElse(null));
        generator.writeObjectField("topicConfigPlans", topic.getTopicConfigPlans());
        if (topic.getPartitionPlan().isPresent()) {
            generator.writeObjectField("partitionPlan", topic.getPartitionPlan().get());
        }
        generator.writeEndObject();
    }
}
