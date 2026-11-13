// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.state.DesiredState
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.util.LogUtil
import com.devshawn.kafka.gitops.util.PlanUtil
import org.apache.kafka.clients.admin.TopicListing
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class PartitionPlanSpec extends Specification {
    TestKafkaService kafka = new TestKafkaService()

    @Unroll
    void 'plans #previous -> #target partitions with configs=#changedConfigs'() {
        given:
        kafka.partitionCounts['test-topic'] = previous
        kafka.entries = [entry('retention.ms', '1000')]
        def details = new TopicDetails.Builder().setPartitions(target).setReplication(1)
                .putConfigs('retention.ms', changedConfigs ? '2000' : '1000').build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(options(null, true), kafka, mapper()).planTopics(
                new DesiredState.Builder().putTopics('test-topic', details).build(), builder)
        def full = builder.build()
        def topic = full.topicPlans.first()

        then:
        topic.action == (target > previous || changedConfigs ? UPDATE : NO_CHANGE)
        topic.partitionPlan.present == (target > previous)
        !topic.partitionPlan.present || (topic.partitionPlan.get().previousCount == previous && topic.partitionPlan.get().targetCount == target)
        topic.topicConfigPlans.first().previousValue.orElse(null) == (changedConfigs ? '1000' : null)
        topic.topicConfigPlans.first().action == (changedConfigs ? UPDATE : NO_CHANGE)
        PlanUtil.getOverview(full, true, true, false).update == (target > previous || changedConfigs ? 1 : 0)
        full.toChangesOnlyPlan().topicPlans.size() == (topic.action == UPDATE ? 1 : 0)
        topic.action != UPDATE || full.toChangesOnlyPlan().topicPlans.first().partitionPlan == topic.partitionPlan
        kafka.partitionChanges.empty

        where:
        previous | target | changedConfigs
        3        | 6      | false
        3        | 3      | false
        3        | 6      | true
        3        | 3      | true
    }

    void 'partition-only update is visible with no configs and survives changes-only filtering'() {
        given:
        kafka.partitionCounts['test-topic'] = 3
        def builder = new DesiredPlan.Builder()
        def desired = new TopicDetails.Builder().setPartitions(6).setReplication(1).build()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(new DesiredState.Builder().putTopics('test-topic', desired).build(), builder)
        def plan = builder.build().toChangesOnlyPlan()
        String text = capture { LogUtil.printPlan(plan, true, true, false) }

        then:
        plan.topicPlans.first().topicConfigPlans.empty
        text.contains('~ [TOPIC] test-topic')
        text.contains('~ partitions: 3 -> 6')
        !text.contains('~ configs:')
        PlanUtil.getOverview(plan, true, true, false).update == 1
    }

    void 'new topic uses creation details and no partition update operation'() {
        given:
        kafka.topics = []
        def builder = new DesiredPlan.Builder()
        def desired = new TopicDetails.Builder().setPartitions(6).setReplication(2).build()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(new DesiredState.Builder().putTopics('orders', desired).build(), builder)
        def topic = builder.build().topicPlans.first()

        then:
        topic.action == ADD
        topic.partitionPlan.empty
        topic.topicDetails.get() == desired
        kafka.metadataReads == [[]]
    }

    @Unroll
    void 'rejects invalid or decreasing partitions #previous -> #target'() {
        given:
        kafka.partitionCounts['test-topic'] = previous
        def desired = new TopicDetails.Builder().setPartitions(target).setReplication(1).build()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(
                new DesiredState.Builder().putTopics('test-topic', desired).build(), new DesiredPlan.Builder())

        then:
        def error = thrown(ValidationException)
        error.message.contains(expected)
        kafka.partitionChanges.empty

        where:
        previous | target | expected
        6        | 3      | 'Decreasing'
        3        | 0      | 'positive integer'
        3        | -1     | 'positive integer'
    }

    void 'fails closed when a listed topic has no description'() {
        given:
        TestKafkaService missing = new TestKafkaService() {
            @Override
            Map describeTopics(List names) { [:] }
        }

        when:
        new PlanManager(options(), missing, mapper()).planTopics(
                new DesiredState.Builder().putTopics('test-topic', details()).build(), new DesiredPlan.Builder())

        then:
        thrown(KafkaExecutionException)
    }

    void 'RF mismatch is warned without creating a reassignment operation'() {
        given:
        kafka.replicationCounts['test-topic'] = 2
        def builder = new DesiredPlan.Builder()

        when:
        String text = capture {
            new PlanManager(options(), kafka, mapper()).planTopics(
                    new DesiredState.Builder().putTopics('test-topic', details()).build(), builder)
        }

        then:
        text.contains('[WARNING]')
        text.contains('replication factors [2], desired 1')
        text.contains('will not be changed')
        builder.build().topicPlans.first().action == NO_CHANGE
        builder.build().topicPlans.first().partitionPlan.empty
    }

    void 'skip-topics performs no topic metadata or mutation calls'() {
        given:
        def skipped = new com.devshawn.kafka.gitops.config.ManagerConfig.Builder().mergeFrom(options())
                .setSkipTopicsDisabled(true).build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(skipped, kafka, mapper()).planTopics(
                new DesiredState.Builder().putTopics('test-topic', details()).build(), builder)
        new ApplyManager(skipped, kafka).applyTopics(new DesiredPlan.Builder().build())

        then:
        builder.build().topicPlans.empty
        kafka.metadataReads.empty
        kafka.partitionChanges.empty
    }

    static String capture(Closure action) {
        def original = System.out
        def bytes = new ByteArrayOutputStream()
        try {
            System.setOut(new PrintStream(bytes, true, 'UTF-8'))
            action.call()
            bytes.toString('UTF-8')
        } finally {
            System.setOut(original)
        }
    }
}
