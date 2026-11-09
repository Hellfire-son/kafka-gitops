package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public final class TopicConfigPlanSerializer extends StdSerializer<TopicConfigPlan> {
    public TopicConfigPlanSerializer() {
        super(TopicConfigPlan.class);
    }

    @Override
    public void serialize(TopicConfigPlan config, JsonGenerator generator, SerializerProvider provider) throws IOException {
        boolean sensitive = TopicConfigSecurity.isSensitive(config);
        if (sensitive && (config.getValue().isPresent() || config.getPreviousValue().isPresent())) {
            throw JsonMappingException.from(generator, "Confidential configuration values cannot be exported. Apply directly from the state file.");
        }
        generator.writeStartObject();
        generator.writeStringField("key", config.getKey());
        generator.writeObjectField("value", config.getValue().orElse(null));
        if (config.getPreviousValue().isPresent()) {
            generator.writeStringField("previousValue", config.getPreviousValue().get());
        }
        generator.writeObjectField("action", config.getAction());
        if (sensitive) {
            generator.writeBooleanField("sensitive", true);
        }
        generator.writeEndObject();
    }
}
