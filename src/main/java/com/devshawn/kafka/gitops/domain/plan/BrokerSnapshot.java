package com.devshawn.kafka.gitops.domain.plan;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = BrokerSnapshot.Builder.class)
public interface BrokerSnapshot {
    int getId();

    Optional<String> getRack();

    class Builder extends BrokerSnapshot_Builder {
    }
}
