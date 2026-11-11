package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsTopicsDefaults.Builder.class)
@JsonDeserialize(builder = SettingsTopicsDefaults.Builder.class)
public interface SettingsTopicsDefaults {

    Optional<Integer> getReplication();

    class Builder extends SettingsTopicsDefaults_Builder {
    }
}
