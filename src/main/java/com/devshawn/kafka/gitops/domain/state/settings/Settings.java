// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = Settings.Builder.class)
@JsonDeserialize(builder = Settings.Builder.class)
public interface Settings {

    Optional<SettingsCCloud> getCcloud();

    Optional<SettingsTopics> getTopics();

    Optional<SettingsServices> getServices();

    Optional<SettingsFiles> getFiles();

    class Builder extends Settings_Builder {
    }
}
