package com.devshawn.kafka.gitops.config;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Map;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = KafkaGitopsConfig.Builder.class)
@JsonDeserialize(builder = KafkaGitopsConfig.Builder.class)
public interface KafkaGitopsConfig {

    Map<String, Object> getConfig();

    class Builder extends KafkaGitopsConfig_Builder {

    }

}
