package com.devshawn.kafka.gitops.domain.state;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;
import java.util.Map;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = DesiredState.Builder.class)
@JsonDeserialize(builder = DesiredState.Builder.class)
public interface DesiredState {

    Map<String, TopicDetails> getTopics();

    Map<String, AclDetails> getAcls();

    List<String> getPrefixedTopicsToIgnore();

    List<String> getPrefixedTopicsToAccept();

    class Builder extends DesiredState_Builder {
    }
}
