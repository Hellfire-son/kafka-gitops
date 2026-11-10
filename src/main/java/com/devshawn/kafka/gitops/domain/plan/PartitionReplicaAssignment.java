package com.devshawn.kafka.gitops.domain.plan;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;

@FreeBuilder
@JsonDeserialize(builder = PartitionReplicaAssignment.Builder.class)
public interface PartitionReplicaAssignment {
    int getPartition();

    List<Integer> getPreviousReplicas();

    List<Integer> getTargetReplicas();

    class Builder extends PartitionReplicaAssignment_Builder {
    }
}
