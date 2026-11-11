package com.devshawn.kafka.gitops.domain.options;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = GetAclOptions.Builder.class)
@JsonDeserialize(builder = GetAclOptions.Builder.class)
public interface GetAclOptions {

    String getServiceName();

    boolean getDescribeAclEnabled();

    class Builder extends GetAclOptions_Builder {
    }
}
