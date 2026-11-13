// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.settings;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsTopics.Builder.class)
@JsonDeserialize(builder = SettingsTopics.Builder.class)
public interface SettingsTopics {

    Optional<SettingsTopicsDefaults> getDefaults();

    @JsonProperty("exclude")
    Optional<SettingsTopicsList> getExcludeList();

    @JsonProperty("include")
    Optional<SettingsTopicsList> getIncludeList();

    class Builder extends SettingsTopics_Builder {
    }
}
