// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy

import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE

/** Synthetic IDs and metadata only. No AdminClient or credentials. */
class ReplicationTestSupport {
    static PartitionReplicaAssignment assignment(int partition, List<Integer> previous, List<Integer> target) {
        new PartitionReplicaAssignment.Builder().setPartition(partition)
                .addAllPreviousReplicas(previous).addAllTargetReplicas(target).build()
    }

    static TopicReplicationPlan replication(List<List<Integer>> previous = [[0], [1]],
                                            List<List<Integer>> target = [[0, 1], [1, 2]], int rf = 2) {
        def builder = new TopicReplicationPlan.Builder().setSchemaVersion(1).setAction(UPDATE)
                .setClusterId('AQIDBAUGBwgJCgsMDQ4PEA').setTopicId('ERITFBUWFxgZGhscHR4fIA')
                .setPartitionCount(previous.size()).setTargetReplicationFactor(rf)
                .setAlgorithm(TopicReplicationPlan.ALGORITHM).setRackPolicy(RackAwarenessPolicy.RACK_AWARE)
                .setExpectedMinInSyncReplicas(1)
        (0..3).each { builder.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(it).setRack('rack-' + it).build()) }
        previous.indices.each { builder.addAssignments(assignment(it, previous[it], target[it])) }
        builder.build()
    }

    static TopicPlan topic(TopicReplicationPlan replication = replication()) {
        new TopicPlan.Builder().setName('orders').setAction(UPDATE).setReplicationPlan(replication).build()
    }

    static DesiredPlan desired(TopicPlan topic = topic()) {
        new DesiredPlan.Builder().addTopicPlans(topic).build()
    }
}
