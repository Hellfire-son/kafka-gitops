package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsCCloud.Builder.class)
@JsonDeserialize(builder = SettingsCCloud.Builder.class)
public interface SettingsCCloud {

    boolean isEnabled();

    class Builder extends SettingsCCloud_Builder {
    }
}
