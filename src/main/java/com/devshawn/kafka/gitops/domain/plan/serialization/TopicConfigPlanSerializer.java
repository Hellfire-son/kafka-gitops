// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.domain.plan.serialization;

import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;


public final class TopicConfigPlanSerializer extends StdSerializer<TopicConfigPlan> {
    public TopicConfigPlanSerializer() {
        super(TopicConfigPlan.class);
    }

    @Override
    public void serialize(TopicConfigPlan config, JsonGenerator generator, SerializationContext provider) {
        boolean sensitive = TopicConfigSecurity.isSensitive(config);
        if (sensitive && (config.getValue().isPresent() || config.getPreviousValue().isPresent())) {
            throw DatabindException.from(generator, "Confidential configuration values cannot be exported. Apply directly from the state file.");
        }
        generator.writeStartObject();
        generator.writeStringProperty("key", config.getKey());
        generator.writePOJOProperty("value", config.getValue().orElse(null));
        if (config.getPreviousValue().isPresent()) {
            generator.writeStringProperty("previousValue", config.getPreviousValue().get());
        }
        generator.writePOJOProperty("action", config.getAction());
        if (sensitive) {
            generator.writeBooleanProperty("sensitive", true);
        }
        generator.writeEndObject();
    }
}
