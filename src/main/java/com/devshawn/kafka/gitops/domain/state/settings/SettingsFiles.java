// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsFiles.Builder.class)
@JsonDeserialize(builder = SettingsFiles.Builder.class)
public interface SettingsFiles {

    Optional<String> getServices();

    Optional<String> getTopics();

    Optional<String> getUsers();

    class Builder extends SettingsFiles_Builder {
    }
}
