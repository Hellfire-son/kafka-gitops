// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.domain.plan;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = PartitionReplicaAssignment.Builder.class)
@JsonDeserialize(builder = PartitionReplicaAssignment.Builder.class)
public interface PartitionReplicaAssignment {
    int getPartition();

    List<Integer> getPreviousReplicas();

    List<Integer> getTargetReplicas();

    class Builder extends PartitionReplicaAssignment_Builder {
    }
}
