// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.confluent;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = ServiceAccount.Builder.class)
@JsonDeserialize(builder = ServiceAccount.Builder.class)
public interface ServiceAccount {

    String getId();

    String getName();

    @JsonIgnoreProperties(ignoreUnknown = true)
    class Builder extends ServiceAccount_Builder {
    }
}
