// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsTopicsList.Builder.class)
@JsonDeserialize(builder = SettingsTopicsList.Builder.class)
public interface SettingsTopicsList {

    List<String> getPrefixed();

    class Builder extends SettingsTopicsList_Builder {
    }
}
