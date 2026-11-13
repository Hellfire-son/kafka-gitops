// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.ReadPlanInputException
import com.devshawn.kafka.gitops.exception.WritePlanOutputException
import com.devshawn.kafka.gitops.util.LogUtil
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class ReplicationJsonSpec extends Specification {
    @TempDir File directory

    @Unroll
    void 'new JSON round trip preserves ordered assignments includeUnchanged=#include'() {
        given:
        def original = desired(new TopicPlan.Builder().mergeFrom(topic())
                .setTopicDetails(new TopicDetails.Builder().setPartitions(2).setReplication(2).build())
                .addTopicConfigPlans(config('retention.ms', UPDATE, '2000', '1000')).build())
        File file = new File(directory, 'plan.json')
        def manager = new PlanManager(options(file, false, include), null, mapper())

        when:
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()

        then:
        restored == original
        restored.topicPlans.first().replicationPlan.get().assignments*.targetReplicas == [[0, 1], [1, 2]]
        restored.topicPlans.first().topicConfigPlans.first().previousValue.get() == '1000'
        !file.text.contains('requiresDecreaseApproval')
        !file.text.contains('hasIncreases')

        where:
        include << [false, true]
    }

    void 'standalone replication JSON round trip preserves decrease'() {
        given:
        def original = replication([[2, 0, 1]], [[2, 1]], 2)

        when:
        def restored = mapper().readValue(mapper().writeValueAsString(original), TopicReplicationPlan)

        then:
        restored == original
        restored.requiresDecreaseApproval()
        !restored.hasIncreases()
    }

    @Unroll
    void 'old JSON retains semantics without replicationPlan: #kind'() {
        given:
        File file = json(content)
        def manager = new PlanManager(options(file), null, mapper())

        when:
        def restored = manager.readPlanFromFile()
        manager.writePlanToFile(restored)

        then:
        restored.topicPlans.first().replicationPlan.empty
        restored.topicPlans.first().partitionPlan.present == (kind == 'partitions')
        !file.text.contains('replicationPlan')
        !file.text.contains('schemaVersion')

        where:
        kind | content
        'config' | '{"topicPlans":[{"name":"orders","action":"UPDATE","topicConfigPlans":[{"key":"retention.ms","action":"UPDATE","value":"2000"}]}],"aclPlans":[]}'
        'partitions' | '{"topicPlans":[{"name":"orders","action":"UPDATE","partitionPlan":{"action":"UPDATE","previousCount":2,"targetCount":3}}],"aclPlans":[]}'
        'creation' | '{"topicPlans":[{"name":"orders","action":"ADD","topicDetails":{"partitions":2,"replication":1}}],"aclPlans":[]}'
    }

    @Unroll
    void 'rejects malformed JSON #description with safe diagnostic'() {
        given:
        def tree = mapper().readValue(mapper().writeValueAsString(desired()), Map)
        mutation(tree.topicPlans.first())
        File file = json(mapper().writeValueAsString(tree))

        when:
        new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        def error = thrown(ReadPlanInputException)
        error.message.contains('replication')
        !error.message.contains('private-json-secret')

        where:
        description | mutation
        'unknown schema' | { it.replicationPlan.schemaVersion = 99 }
        'missing schema' | { it.replicationPlan.remove('schemaVersion') }
        'unknown operation' | { it.replicationPlan.action = 'private-json-secret' }
        'ADD operation' | { it.replicationPlan.action = 'ADD' }
        'REMOVE operation' | { it.replicationPlan.action = 'REMOVE' }
        'NO_CHANGE operation' | { it.replicationPlan.action = 'NO_CHANGE' }
        'numeric operation' | { it.replicationPlan.action = 1 }
        'missing operation' | { it.replicationPlan.remove('action') }
        'unknown rack policy' | { it.replicationPlan.rackPolicy = 'private-json-secret' }
        'missing rack policy' | { it.replicationPlan.remove('rackPolicy') }
        'null RF' | { it.replicationPlan.targetReplicationFactor = null }
        'fraction RF' | { it.replicationPlan.targetReplicationFactor = 2.5 }
        'string RF' | { it.replicationPlan.targetReplicationFactor = '2' }
        'overflow RF' | { it.replicationPlan.targetReplicationFactor = 2147483648L }
        'missing RF' | { it.replicationPlan.remove('targetReplicationFactor') }
        'missing partition count' | { it.replicationPlan.remove('partitionCount') }
        'missing cluster' | { it.replicationPlan.remove('clusterId') }
        'missing topic UUID' | { it.replicationPlan.remove('topicId') }
        'null UUID' | { it.replicationPlan.topicId = null }
        'invalid UUID' | { it.replicationPlan.topicId = 'private-json-secret' }
        'missing algorithm' | { it.replicationPlan.remove('algorithm') }
        'missing min ISR' | { it.replicationPlan.remove('expectedMinInSyncReplicas') }
        'missing snapshot' | { it.replicationPlan.remove('brokerSnapshot') }
        'null snapshot' | { it.replicationPlan.brokerSnapshot = null }
        'null snapshot broker' | { it.replicationPlan.brokerSnapshot = [null] }
        'missing broker ID' | { it.replicationPlan.brokerSnapshot.first().remove('id') }
        'null broker ID' | { it.replicationPlan.brokerSnapshot.first().id = null }
        'null rack under RACK_AWARE' | { it.replicationPlan.brokerSnapshot.first().rack = null }
        'unknown broker field' | { it.replicationPlan.brokerSnapshot.first().password = 'private-json-secret' }
        'missing assignments' | { it.replicationPlan.remove('assignments') }
        'null assignments' | { it.replicationPlan.assignments = null }
        'null assignment' | { it.replicationPlan.assignments = [null] }
        'missing partition ID' | { it.replicationPlan.assignments.first().remove('partition') }
        'missing previous' | { it.replicationPlan.assignments.first().remove('previousReplicas') }
        'missing target' | { it.replicationPlan.assignments.first().remove('targetReplicas') }
        'null previous broker' | { it.replicationPlan.assignments.first().previousReplicas = [null] }
        'unknown assignment field' | { it.replicationPlan.assignments.first().token = 'private-json-secret' }
        'incomplete assignments' | { it.replicationPlan.assignments.remove(0) }
        'duplicate partition' | { it.replicationPlan.assignments.last().partition = 0 }
        'duplicate broker' | { it.replicationPlan.assignments.first().targetReplicas = [0, 0] }
        'credentials' | { it.replicationPlan.credentials = ['sasl.jaas.config':'private-json-secret'] }
        'fake approval' | { it.replicationPlan.requiresDecreaseApproval = false }
        'fake allow-decrease' | { it.replicationPlan.allowDecrease = true }
        'null plan' | { it.replicationPlan = null }
        'array plan' | { it.replicationPlan = [] }
        'string plan' | { it.replicationPlan = 'private-json-secret' }
        'hidden NO_CHANGE' | { it.action = 'NO_CHANGE' }
        'partition conflict' | { it.partitionPlan = [action:'UPDATE', previousCount:2, targetCount:3] }
    }

    @Unroll
    void 'refuses secret export before opening file location=#location includeUnchanged=#include'() {
        given:
        def builder = new TopicPlan.Builder().mergeFrom(topic())
        if (location == 'details') {
            builder.setTopicDetails(new TopicDetails.Builder().setPartitions(2).setReplication(2)
                    .putConfigs('sasl.jaas.config', 'private-export-secret').build())
        } else {
            builder.addTopicConfigPlans(config('ssl.key.password', UPDATE, 'private-export-secret', 'private-old-secret', true))
        }
        File file = json('previous-plan')

        when:
        new PlanManager(options(file, false, include), null, mapper()).writePlanToFile(desired(builder.build()))

        then:
        def error = thrown(WritePlanOutputException)
        !error.message.contains('private-')
        file.text == 'previous-plan'

        where:
        [location, include] << [['details', 'config'], [false, true]].combinations()
    }

    @Unroll
    void 'refuses invalid model export before truncation includeUnchanged=#include'() {
        given:
        def invalid = desired(topic(new TopicReplicationPlan.Builder().mergeFrom(replication()).setSchemaVersion(2).build()))
        File file = json('previous-plan')

        when:
        new PlanManager(options(file, false, include), null, mapper()).writePlanToFile(invalid)

        then:
        thrown(WritePlanOutputException)
        file.text == 'previous-plan'

        where:
        include << [false, true]
    }

    void 'JSON has only allowlisted metadata and no credential containers'() {
        when:
        def tree = mapper().readValue(mapper().writeValueAsString(desired()), Map).topicPlans.first().replicationPlan

        then:
        tree.keySet() == ['schemaVersion','action','clusterId','topicId','partitionCount','targetReplicationFactor',
                          'algorithm','rackPolicy','expectedMinInSyncReplicas','brokerSnapshot','assignments'].toSet()
        tree.brokerSnapshot.every { it.keySet() == ['id','rack'].toSet() }
        tree.assignments.every { it.keySet() == ['partition','previousReplicas','targetReplicas'].toSet() }
    }

    void 'RF plan display keeps sensitive config values redacted'() {
        given:
        def sensitive = desired(new TopicPlan.Builder().mergeFrom(topic()).addTopicConfigPlans(
                config('sasl.jaas.config', UPDATE, 'private-display-jaas', 'private-display-old', true),
                config('custom.extension', UPDATE, 'private-display-custom', null, true)).build())

        when:
        String output = PartitionPlanSpec.capture { LogUtil.printPlan(sensitive, false, false, false) }

        then:
        !output.contains('private-display')
        output.contains('[redacted]')
    }

    @Unroll
    void 'strict reader rejects #description without ignoring RF input'() {
        given:
        String valid = mapper().writeValueAsString(desired())
        File file = json(mutation(valid))

        when:
        new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        thrown(ReadPlanInputException)

        where:
        description | mutation
        'duplicate schemaVersion' | { it.replace('"schemaVersion":1', '"schemaVersion":1,"schemaVersion":1') }
        'trailing JSON' | { it + '{}' }
        'unknown root field' | { it.replace('"topicPlans":', '"unknown":true,"topicPlans":') }
    }

    private File json(String content) {
        File file = new File(directory, 'input.json')
        file.setText(content, 'UTF-8')
        file
    }
}
