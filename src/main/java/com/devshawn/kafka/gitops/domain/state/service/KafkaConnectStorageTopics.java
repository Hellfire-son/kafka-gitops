// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.state.service;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = KafkaConnectStorageTopics.Builder.class)
@JsonDeserialize(builder = KafkaConnectStorageTopics.Builder.class)
public interface KafkaConnectStorageTopics {

    Optional<String> getConfig();

    Optional<String> getOffset();

    Optional<String> getStatus();

    class Builder extends KafkaConnectStorageTopics_Builder {
    }
}
