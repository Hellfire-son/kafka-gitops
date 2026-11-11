package com.devshawn.kafka.gitops.domain.plan;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = BrokerSnapshot.Builder.class)
@JsonDeserialize(builder = BrokerSnapshot.Builder.class)
public interface BrokerSnapshot {
    int getId();

    Optional<String> getRack();

    class Builder extends BrokerSnapshot_Builder {
    }
}
