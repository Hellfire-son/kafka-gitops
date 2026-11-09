package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.state.DesiredState
import com.devshawn.kafka.gitops.service.ParserService
import com.devshawn.kafka.gitops.util.LogUtil
import com.devshawn.kafka.gitops.util.PlanUtil
import org.apache.kafka.clients.admin.TopicListing
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*

// Reuses golden files with an in-memory Kafka service, never the integration test lifecycle.
@Unroll
class PlanFixturesSpec extends Specification {
    void 'generated topic plan matches #fixture golden file'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        kafka.topics = ['delete-topic', 'test-topic', 'topic-with-configs-1', 'topic-with-configs-2']
                .collect { new TopicListing(it, false) }
        kafka.topicEntries = [
                'topic-with-configs-1': [entry('cleanup.policy', 'compact'), entry('segment.bytes', '100000')],
                'topic-with-configs-2': [entry('retention.ms', '60000')]
        ]
        def stateFile = new ParserService(resource(yaml + '.yaml')).parseStateFile()
        def state = new DesiredState.Builder().putAllTopics(stateFile.topics).build()
        // These golden cases exercise configs. Keep their partition metadata aligned with their input;
        // partition changes and shrink rejection are covered independently in PartitionPlanSpec.
        state.topics.each { name, details -> kafka.partitionCounts[name] = details.partitions }
        def builder = new DesiredPlan.Builder()
        def expected = mapper().readValue(resource(fixture + '-plan.json'), DesiredPlan)

        when:
        new PlanManager(options(null, noDelete), kafka, mapper()).planTopics(state, builder)
        def actual = includeUnchanged ? builder.build() : builder.build().toChangesOnlyPlan()

        then:
        actual.topicPlans.collectEntries { [(it.name): it] } == expected.topicPlans.collectEntries { [(it.name): it] }

        where:
        fixture                        | yaml                        | includeUnchanged | noDelete
        'seed-basic'                   | 'seed-basic'                | false            | false
        'seed-basic-include-unchanged' | 'seed-basic'                | true             | false
        'seed-acl-exists'               | 'seed-acl-exists'            | false            | true
        'seed-topic-modification-2'     | 'seed-topic-modification-2' | false            | false
        'seed-topic-modification-3'     | 'seed-topic-modification-3' | false            | false
    }

    void 'in-memory apply matches #fixture output with no-delete=#noDelete'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        def desired = new PlanManager(options(resource(fixture + '-plan.json')), kafka, mapper()).readPlanFromFile()
        def manager = new ApplyManager(options(null, noDelete), kafka)
        PrintStream previous = System.out
        ByteArrayOutputStream bytes = new ByteArrayOutputStream()
        String expectedName = fixture + (noDelete ? '-no-delete-apply-output.txt' : '-apply-output.txt')

        when:
        try {
            System.setOut(new PrintStream(bytes, true, 'UTF-8'))
            System.out.println('Executing apply...\n')
            if (!skipTopics) {
                manager.applyTopics(desired)
            }
            if (!skipAcls) {
                manager.applyAcls(desired)
            }
            LogUtil.printApplyOverview(PlanUtil.getOverview(desired, noDelete, skipAcls, skipTopics))
        } finally {
            System.setOut(previous)
        }

        then:
        bytes.toString('UTF-8').replace('\r\n', '\n') == resource(expectedName).text.replace('\r\n', '\n')

        where:
        fixture                     | noDelete | skipAcls | skipTopics
        'seed-acl-exists'            | false    | false    | false
        'seed-topic-modification'    | false    | false    | false
        'seed-topic-modification'    | true     | false    | false
        'seed-topic-modification-3'  | false    | false    | false
        'seed-topic-modification-3'  | true     | false    | false
        'skip-acls-apply'            | false    | true     | false
        'skip-topics-apply'          | false    | false    | true
    }

    private File resource(String name) {
        new File(getClass().getResource('/plans/' + name).toURI())
    }
}
