// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.config;

import tools.jackson.databind.annotation.JsonDeserialize;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import org.inferred.freebuilder.FreeBuilder;

import java.io.File;
import java.util.Optional;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = ManagerConfig.Builder.class)
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
