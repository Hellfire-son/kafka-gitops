// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.ReadPlanInputException
import com.devshawn.kafka.gitops.exception.WritePlanOutputException
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.manager.PartitionApplySpec.update
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class PartitionJsonSpec extends Specification {
    @TempDir File directory

    void 'partition and config operations round trip without losing previousValue'() {
        given:
        def original = new DesiredPlan.Builder().addTopicPlans(update('test-topic', 3, 6,
                [config('retention.ms', UPDATE, '2000', '1000')])).build()
        File file = new File(directory, 'plan.json')
        def manager = new PlanManager(options(file), null, mapper())

        when:
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()

        then:
        restored == original
        restored.topicPlans.first().partitionPlan.get().previousCount == 3
        restored.topicPlans.first().partitionPlan.get().targetCount == 6
        restored.topicPlans.first().topicConfigPlans.first().previousValue.get() == '1000'
        file.text.contains('partitionPlan')
    }

    void 'legacy config-only JSON never infers partition writes from topicDetails'() {
        given:
        File file = json('''{"topicPlans":[{"name":"test-topic","action":"UPDATE",
          "topicDetails":{"partitions":6,"replication":1,"configs":{}},
          "topicConfigPlans":[{"key":"retention.ms","action":"UPDATE","value":"2000"}]}],"aclPlans":[]}''')
        def kafka = new TestKafkaService(partitionCounts: ['test-topic': 3])

        when:
        def restored = new PlanManager(options(file), kafka, mapper()).readPlanFromFile()
        new ApplyManager(options(), kafka).applyTopics(restored)

        then:
        restored.topicPlans.first().partitionPlan.empty
        kafka.partitionChanges.empty
        kafka.operations.size() == 1
    }

    @Unroll
    void 'rejects malformed partition operation #description'() {
        given:
        File file = json('''{"topicPlans":[{"name":"x","action":"UPDATE","partitionPlan":''' + operation + '}]}')

        when:
        new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        thrown(ReadPlanInputException)

        where:
        description        | operation
        'unknown action'   | '{"action":"REASSIGN","previousCount":3,"targetCount":6}'
        'ADD action'       | '{"action":"ADD","previousCount":3,"targetCount":6}'
        'REMOVE action'    | '{"action":"REMOVE","previousCount":3,"targetCount":6}'
        'NO_CHANGE action' | '{"action":"NO_CHANGE","previousCount":3,"targetCount":6}'
        'missing action'   | '{"previousCount":3,"targetCount":6}'
        'missing previous' | '{"action":"UPDATE","targetCount":6}'
        'missing target'   | '{"action":"UPDATE","previousCount":3}'
        'zero target'      | '{"action":"UPDATE","previousCount":3,"targetCount":0}'
        'zero previous'    | '{"action":"UPDATE","previousCount":0,"targetCount":6}'
        'decrease'         | '{"action":"UPDATE","previousCount":6,"targetCount":3}'
        'same counts'      | '{"action":"UPDATE","previousCount":3,"targetCount":3}'
        'null target'      | '{"action":"UPDATE","previousCount":3,"targetCount":null}'
        'fraction target'  | '{"action":"UPDATE","previousCount":3,"targetCount":6.5}'
        'string target'    | '{"action":"UPDATE","previousCount":3,"targetCount":"6"}'
        'negative previous'| '{"action":"UPDATE","previousCount":-1,"targetCount":6}'
        'unknown field'    | '{"action":"UPDATE","previousCount":3,"targetCount":6,"replication":3}'
        'wrong structure'  | '[]'
        'null operation'   | 'null'
        'overflow'         | '{"action":"UPDATE","previousCount":3,"targetCount":2147483648}'
    }

    @Unroll
    void 'rejects partition operation hidden by topic action #action'() {
        given:
        File file = json('''{"topicPlans":[{"name":"x","action":"''' + action + '''",
          "topicDetails":{"partitions":6,"replication":1,"configs":{}},
          "partitionPlan":{"action":"UPDATE","previousCount":3,"targetCount":6}}]}''')

        when:
        new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        thrown(ReadPlanInputException)

        where:
        action << ['ADD','REMOVE','NO_CHANGE']
    }

    void 'partition update cannot smuggle secrets through configs or topicDetails'() {
        given:
        File file = new File(directory, 'protected.json')
        file.text = 'existing-plan'
        def topic = new TopicPlan.Builder().mergeFrom(update()).setTopicDetails(
                new com.devshawn.kafka.gitops.domain.state.TopicDetails.Builder().setPartitions(6).setReplication(1)
                        .putConfigs('sasl.jaas.config', 'private-partition-secret').build()).build()

        when:
        new PlanManager(options(file), null, mapper()).writePlanToFile(new DesiredPlan.Builder().addTopicPlans(topic).build())

        then:
        def error = thrown(WritePlanOutputException)
        !error.message.contains('private-partition')
        file.text == 'existing-plan'
    }

    @Unroll
    void 'config operations cannot hide an invalid partition structure #description'() {
        given:
        File file = json(content)

        when:
        new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        thrown(ReadPlanInputException)

        where:
        description | content
        'null' | '{"topicPlans":[{"name":"x","action":"UPDATE","partitionPlan":null,"topicConfigPlans":[{"key":"retention.ms","action":"UPDATE","value":"1"}]}]}'
        'different target' | '{"topicPlans":[{"name":"x","action":"UPDATE","topicDetails":{"partitions":3},"partitionPlan":{"action":"UPDATE","previousCount":3,"targetCount":6}}]}'
        'duplicate topics' | '{"topicPlans":[{"name":"x","action":"UPDATE","partitionPlan":{"action":"UPDATE","previousCount":3,"targetCount":6}},{"name":"x","action":"REMOVE"}]}'
    }

    private File json(String content) {
        File file = new File(directory, 'input.json')
        file.text = content
        file
    }
}
