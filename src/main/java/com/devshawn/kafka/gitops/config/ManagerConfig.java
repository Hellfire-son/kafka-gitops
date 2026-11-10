package com.devshawn.kafka.gitops.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import org.inferred.freebuilder.FreeBuilder;

import java.io.File;
import java.util.Optional;

@FreeBuilder
@JsonDeserialize(builder = ManagerConfig.Builder.class)
public interface ManagerConfig {

    boolean isVerboseRequested();

    boolean isDeleteDisabled();

    boolean isIncludeUnchangedEnabled();

    boolean isSkipAclsDisabled();

    boolean isSkipTopicsDisabled();

    Optional<File> getConfigFile();

    File getStateFile();

    Optional<File> getPlanFile();

    boolean isManageReplicationRequested();

    boolean isAllowReplicationDecreaseRequested();

    RackAwarenessPolicy getReplicationRackPolicy();

    class Builder extends ManagerConfig_Builder {
        public Builder() {
            setManageReplicationRequested(false);
            setAllowReplicationDecreaseRequested(false);
            setReplicationRackPolicy(RackAwarenessPolicy.RACK_AWARE);
        }
    }
}
