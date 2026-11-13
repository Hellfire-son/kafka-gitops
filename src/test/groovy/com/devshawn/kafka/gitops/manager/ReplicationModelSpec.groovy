// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy
import com.devshawn.kafka.gitops.util.PlanValidation
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.config
import static com.devshawn.kafka.gitops.ReplicationTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class ReplicationModelSpec extends Specification {
    @Unroll
    void 'models per-partition RF and decrease risk: #previous -> #target'() {
        given:
        def rf = replication(previous, target, target.first().size())

        when:
        PlanValidation.validate(desired(topic(rf)))

        then:
        rf.requiresDecreaseApproval() == decrease
        rf.hasIncreases() == increase
        rf.assignments*.previousReplicas == previous
        rf.assignments*.targetReplicas == target

        where:
        previous           | target             | decrease | increase
        [[0], [1]]         | [[0, 1], [1, 2]]   | false    | true
        [[0, 1, 2], [1, 2, 0]] | [[0, 2], [1, 0]] | true | false
        [[0], [1, 2]]      | [[0, 1], [1, 2]]   | false    | true
        [[0], [1, 2, 3]]   | [[0, 1], [1, 3]]   | true     | true
    }

    @Unroll
    void 'rejects invalid replication model: #description'() {
        given:
        def builder = new TopicReplicationPlan.Builder().mergeFrom(replication())
        mutation(builder)

        when:
        ReplicationPlanValidation.validate(builder.build())

        then:
        def error = thrown(ReplicationPlanValidation.InvalidReplicationPlanException)
        error.message.contains(message)

        where:
        description | mutation | message
        'schema' | { it.setSchemaVersion(2) } | 'schemaVersion'
        'ADD' | { it.setAction(ADD) } | 'UPDATE'
        'REMOVE' | { it.setAction(REMOVE) } | 'UPDATE'
        'NO_CHANGE' | { it.setAction(NO_CHANGE) } | 'UPDATE'
        'zero RF' | { it.setTargetReplicationFactor(0) } | 'positive'
        'negative RF' | { it.setTargetReplicationFactor(-1) } | 'positive'
        'overflow RF' | { it.setTargetReplicationFactor(32768) } | '32767'
        'zero partitions' | { it.setPartitionCount(0) } | 'partitionCount'
        'negative partitions' | { it.setPartitionCount(-1) } | 'partitionCount'
        'missing partition' | { it.setPartitionCount(3) } | 'cover every partition'
        'too many partitions' | { it.setPartitionCount(1) } | 'cover every partition'
        'empty assignments' | { it.clearAssignments() } | 'cover every partition'
        'duplicate partition' | { it.clearAssignments().addAssignments(assignment(0,[0],[0,1]), assignment(0,[1],[1,2])) } | 'duplicate partition'
        'negative partition' | { it.clearAssignments().addAssignments(assignment(-1,[0],[0,1]), assignment(1,[1],[1,2])) } | 'outside'
        'out of range partition' | { it.clearAssignments().addAssignments(assignment(0,[0],[0,1]), assignment(2,[1],[1,2])) } | 'outside'
        'duplicate target broker' | { it.clearAssignments().addAssignments(assignment(0,[0],[0,0]), assignment(1,[1],[1,2])) } | 'duplicate broker'
        'duplicate previous broker' | { it.clearAssignments().addAssignments(assignment(0,[0,0],[0,1]), assignment(1,[1],[1,2])) } | 'duplicate broker'
        'unknown broker' | { it.clearAssignments().addAssignments(assignment(0,[0],[0,99]), assignment(1,[1],[1,2])) } | 'brokerSnapshot'
        'negative broker' | { it.clearAssignments().addAssignments(assignment(0,[-1],[-1,1]), assignment(1,[1],[1,2])) } | 'non-negative'
        'empty previous' | { it.clearAssignments().addAssignments(assignment(0,[],[0,1]), assignment(1,[1],[1,2])) } | 'empty'
        'empty target' | { it.clearAssignments().addAssignments(assignment(0,[0],[]), assignment(1,[1],[1,2])) } | 'empty'
        'wrong target size' | { it.clearAssignments().addAssignments(assignment(0,[0],[0,1,2]), assignment(1,[1],[1,2])) } | 'target RF size'
        'rebalance at same RF' | { it.clearAssignments().addAssignments(assignment(0,[0,1],[0,2]), assignment(1,[1],[1,2])) } | 'unchanged RF'
        'reorder at same RF' | { it.clearAssignments().addAssignments(assignment(0,[0,1],[1,0]), assignment(1,[1],[1,2])) } | 'unchanged RF'
        'reorder at increase' | { it.clearAssignments().addAssignments(assignment(0,[0],[1,0]), assignment(1,[1],[1,2])) } | 'prefix'
        'replacement at decrease' | { it.clearAssignments().addAssignments(assignment(0,[0,1,2],[0,3]), assignment(1,[1],[1,2])) } | 'ordered subset'
        'reorder at decrease' | { it.clearAssignments().addAssignments(assignment(0,[0,1,2],[2,0]), assignment(1,[1],[1,2])) } | 'ordered subset'
        'all unchanged' | { it.clearAssignments().addAssignments(assignment(0,[0,1],[0,1]), assignment(1,[1,2],[1,2])) } | 'actual RF change'
        'unknown algorithm' | { it.setAlgorithm('random') } | 'algorithm'
        'min ISR zero' | { it.setExpectedMinInSyncReplicas(0) } | 'expectedMinInSyncReplicas'
        'min ISR above target' | { it.setExpectedMinInSyncReplicas(3) } | 'expectedMinInSyncReplicas'
        'missing brokers' | { it.clearBrokerSnapshot() } | 'too few'
        'negative snapshot broker' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(-1).setRack('r').build()) } | 'non-negative'
        'duplicate snapshot broker' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(0).setRack('r').build()) } | 'duplicate broker'
        'missing rack' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(4).build()) } | 'rack label'
        'blank rack' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(4).setRack(' ').build()) } | 'non-blank'
        'control rack' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(4).setRack('r\n').build()) } | 'control'
        'long rack' | { it.addBrokerSnapshot(new BrokerSnapshot.Builder().setId(4).setRack('r' * 256).build()) } | '255'
    }

    @Unroll
    void 'rejects invalid #field UUID without echoing input #value'() {
        given:
        def builder = new TopicReplicationPlan.Builder().mergeFrom(replication())
        field == 'clusterId' ? builder.setClusterId(value) : builder.setTopicId(value)

        when:
        ReplicationPlanValidation.validate(builder.build())

        then:
        def error = thrown(ReplicationPlanValidation.InvalidReplicationPlanException)
        error.message.contains(field)
        !error.message.contains(value ?: 'private-secret')

        where:
        [field, value] << [['clusterId', 'topicId'], ['', 'private-secret', '00000000-0000-0000-0000-000000000001',
                          'AAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAQ', 'AQIDBAUGBwgJCgsMDQ4PEB']].combinations()
    }

    void 'BROKER_ONLY permits missing racks with explicit policy'() {
        given:
        def rf = new TopicReplicationPlan.Builder().mergeFrom(replication()).setRackPolicy(RackAwarenessPolicy.BROKER_ONLY)
                .clearBrokerSnapshot().addAllBrokerSnapshot((0..3).collect { new BrokerSnapshot.Builder().setId(it).build() }).build()

        when:
        ReplicationPlanValidation.validate(rf)

        then:
        noExceptionThrown()
    }

    @Unroll
    void 'RF operation cannot be hidden by parent action #action'() {
        when:
        PlanValidation.validate(desired(new TopicPlan.Builder().mergeFrom(topic()).setAction(action).build()))

        then:
        thrown(ReplicationPlanValidation.InvalidReplicationPlanException)

        where:
        action << [ADD, REMOVE, NO_CHANGE]
    }

    void 'rejects partition and replication operations together'() {
        given:
        def conflict = new TopicPlan.Builder().mergeFrom(topic()).setPartitionPlan(
                new TopicPartitionPlan.Builder().setAction(UPDATE).setPreviousCount(2).setTargetCount(3).build()).build()

        when:
        PlanValidation.validate(desired(conflict))

        then:
        def error = thrown(ReplicationPlanValidation.InvalidReplicationPlanException)
        error.message.contains('cannot be combined')
    }

    @Unroll
    void 'rejects mismatched topicDetails partitions=#partitions RF=#rf'() {
        when:
        PlanValidation.validate(desired(new TopicPlan.Builder().mergeFrom(topic()).setTopicDetails(
                new TopicDetails.Builder().setPartitions(partitions).setReplication(rf).build()).build()))

        then:
        thrown(ReplicationPlanValidation.InvalidReplicationPlanException)

        where:
        partitions | rf
        3          | 2
        2          | 1
    }

    void 'filtering preserves complete RF snapshot and previous config values'() {
        given:
        def original = desired(new TopicPlan.Builder().mergeFrom(topic())
                .addTopicConfigPlans(config('retention.ms', UPDATE, '2000', '1000'), config('cleanup.policy', NO_CHANGE, 'delete')).build())

        when:
        def filtered = original.toChangesOnlyPlan()

        then:
        filtered.topicPlans.first().replicationPlan == original.topicPlans.first().replicationPlan
        filtered.topicPlans.first().topicConfigPlans*.key == ['retention.ms']
        filtered.topicPlans.first().topicConfigPlans.first().previousValue.get() == '1000'
    }

    void 'RF-only operation survives topic and root changes-only filtering'() {
        expect:
        topic().toChangesOnlyPlan() == topic()
        desired().toChangesOnlyPlan() == desired()
    }

    @Unroll
    void 'rejects invalid Kafka topic name #name'() {
        when:
        PlanValidation.validate(desired(new TopicPlan.Builder().mergeFrom(topic()).setName(name).build()))

        then:
        thrown(ReplicationPlanValidation.InvalidReplicationPlanException)

        where:
        name << ['', '.', '..', 'with space', 'x' * 250]
    }

    void 'filtering rejects RF operation hidden behind NO_CHANGE before dropping it'() {
        when:
        desired(new TopicPlan.Builder().mergeFrom(topic()).setAction(NO_CHANGE).build()).toChangesOnlyPlan()

        then:
        thrown(ReplicationPlanValidation.InvalidReplicationPlanException)
    }
}
