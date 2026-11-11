package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.plan.TopicPartitionPlan
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import org.apache.kafka.clients.admin.TopicListing
import spock.lang.Specification

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*
import static com.devshawn.kafka.gitops.manager.PartitionPlanSpec.capture

class PartitionApplySpec extends Specification {
    static TopicPlan update(String name = 'test-topic', int previous = 3, int target = 6, List configs = []) {
        new TopicPlan.Builder().setName(name).setAction(UPDATE)
                .setPartitionPlan(new TopicPartitionPlan.Builder().setAction(UPDATE).setPreviousCount(previous).setTargetCount(target).build())
                .addAllTopicConfigPlans(configs).build()
    }

    void 'no-delete permits partition increase and leaves topic deletion blocked'() {
        given:
        def kafka = new TestKafkaService(partitionCounts: ['test-topic': 3])
        def plan = new DesiredPlan.Builder().addTopicPlans(update())
                .addTopicPlans(new TopicPlan.Builder().setName('keep').setAction(REMOVE).build()).build()

        when:
        new ApplyManager(options(null, true), kafka).applyTopics(plan)

        then:
        kafka.partitionChanges == [[name: 'test-topic', previousCount: 3, targetCount: 6]]
        kafka.partitionCounts['test-topic'] == 6
        kafka.deletedTopics.empty
        kafka.operations.empty
    }

    void 'repeated partition apply is a fresh-read no-op with an explicit message'() {
        given:
        def kafka = new TestKafkaService(partitionCounts: ['test-topic': 3])
        def plan = new DesiredPlan.Builder().addTopicPlans(update()).build()
        def apply = new ApplyManager(options(), kafka)
        apply.applyTopics(plan)

        when:
        String text = capture { apply.applyTopics(plan) }

        then:
        kafka.partitionChanges.size() == 1
        kafka.metadataReads.count { it == ['test-topic'] } == 2
        text.contains('already has 6 partitions')
        text.contains('no-op')
    }

    void 'preflight detects drift in a later topic before any mutation'() {
        given:
        def kafka = new TestKafkaService(partitionCounts: ['test-topic': 3, 'drifted': 4])
        kafka.topics.add(new TopicListing('drifted', org.apache.kafka.common.Uuid.ZERO_UUID, false))
        def plan = new DesiredPlan.Builder()
                .addTopicPlans(new TopicPlan.Builder().setName('new-topic').setAction(ADD).setTopicDetails(details()).build())
                .addTopicPlans(update('test-topic', 3, 6, [config('retention.ms', UPDATE, '2000', '1000')]))
                .addTopicPlans(update('drifted')).build()

        when:
        String text = capture { new ApplyManager(options(), kafka).applyTopics(plan) }

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('has 4 partitions')
        kafka.partitionChanges.empty
        kafka.createdTopics.empty
        kafka.operations.empty
    }

    void 'partition verification failure prevents config changes and success output'() {
        given:
        def kafka = new TestKafkaService() {
            @Override
            boolean increaseTopicPartitions(String name, int oldCount, int target) {
                throw new KafkaExecutionException('Verification failed.', 'Re-plan before retrying.')
            }
        }
        kafka.partitionCounts = ['test-topic': 3]
        def plan = new DesiredPlan.Builder().addTopicPlans(update('test-topic', 3, 6,
                [config('retention.ms', UPDATE, '2000', '1000')])).build()
        def bytes = new ByteArrayOutputStream()
        def original = System.out

        when:
        try {
            System.setOut(new PrintStream(bytes))
            new ApplyManager(options(), kafka).applyTopics(plan)
        } finally {
            System.setOut(original)
        }

        then:
        thrown(KafkaExecutionException)
        kafka.operations.empty
        !bytes.toString('UTF-8').contains('Successfully applied')
    }

    void 'configs apply after the partition result and confidential values stay in memory'() {
        given:
        def kafka = new TestKafkaService() {
            @Override
            void updateTopicConfig(Map configs) {
                assert partitionCounts['test-topic'] == 6
                super.updateTopicConfig(configs)
            }
        }
        kafka.partitionCounts = ['test-topic': 3]
        def plan = new DesiredPlan.Builder().addTopicPlans(update('test-topic', 3, 6,
                [config('retention.ms', UPDATE, '2000', '1000'),
                 config('custom.password', UPDATE, 'private-stage5-config', null, true),
                 config('cleanup.policy', NO_CHANGE, 'delete')])).build()

        when:
        String text = capture { new ApplyManager(options(), kafka).applyTopics(plan) }

        then:
        kafka.partitionChanges.size() == 1
        kafka.operations.size() == 2
        kafka.operations*.operation*.configEntry()*.value().contains('private-stage5-config')
        !text.contains('private-stage5')
        text.contains('[redacted]')
        text.contains('Successfully applied')
    }

    void 'a later failure preserves confirmed earlier changes and stops subsequent operations'() {
        given:
        def kafka = new TestKafkaService() {
            @Override
            boolean increaseTopicPartitions(String name, int oldCount, int target) {
                if (name == 'second') {
                    throw new KafkaExecutionException('Request failed.', 'Outcome unknown; re-plan.')
                }
                super.increaseTopicPartitions(name, oldCount, target)
            }
        }
        kafka.partitionCounts = ['test-topic': 3, 'second': 3]
        kafka.topics.add(new TopicListing('second', org.apache.kafka.common.Uuid.ZERO_UUID, false))
        def plan = new DesiredPlan.Builder().addTopicPlans(update(), update('second'))
                .addTopicPlans(new TopicPlan.Builder().setName('new-topic').setAction(ADD).setTopicDetails(details()).build()).build()

        when:
        new ApplyManager(options(), kafka).applyTopics(plan)

        then:
        thrown(KafkaExecutionException)
        kafka.partitionCounts['test-topic'] == 6
        kafka.partitionCounts['second'] == 3
        kafka.createdTopics.empty
    }
}
