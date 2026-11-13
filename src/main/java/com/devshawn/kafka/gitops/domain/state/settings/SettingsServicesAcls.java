// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.settings;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = SettingsServicesAcls.Builder.class)
@JsonDeserialize(builder = SettingsServicesAcls.Builder.class)
public interface SettingsServicesAcls {

    Optional<Boolean> getDescribeTopicEnabled();

    class Builder extends SettingsServicesAcls_Builder {
    }
}
