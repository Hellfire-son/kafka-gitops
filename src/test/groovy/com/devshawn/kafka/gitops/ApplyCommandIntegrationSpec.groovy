package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import org.apache.kafka.clients.admin.Admin
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.enums.PlanAction.*

@Unroll
class ApplyCommandIntegrationSpec extends Specification {
    private static final Map<String, List<String>> APPLY_REPORTS = PlanTestSupport.mapper().readValue(
            TestUtils.getResourceFileContent('plans/apply-cli-output.json'), Map)

    @Shared Admin admin
    @TempDir File directory

    void setupSpec() {
        admin = TestUtils.openFixtureAdmin()
    }

    void setup() {
        TestUtils.cleanUpCluster(admin)
    }

    void cleanupSpec() {
        TestUtils.closeFixtureAdmin(admin)
    }

    void 'test various successful applies - #planFile'() {
        when:
        def result = applySaved(planFile)

        then:
        result.code == 0
        result.out == expectedReport(planFile)

        where:
        planFile << [
                'simple',
                'application-service',
                'kafka-streams-service',
                'kafka-connect-service',
                'multi-file',
                'simple-users',
                'custom-service-acls',
                'custom-user-acls',
                'custom-group-id-application',
                'custom-group-id-connect',
                'custom-application-id-streams',
                'custom-storage-topic',
                'custom-storage-topics'
        ]
    }

    void 'test skip-acls flag'() {
        when:
        def result = applySaved(planFile, ['--skip-acls'])

        then:
        result.code == 0
        result.out == expectedReport(planFile)

        where:
        planFile << ['skip-acls-apply']
    }

    void 'test skip-topics flag'() {
        when:
        def result = applySaved(planFile, ['--skip-topics'])

        then:
        result.code == 0
        result.out == expectedReport(planFile)

        where:
        planFile << ['skip-topics-apply']
    }

    void 'test various valid applies with seed - #planFile #deleteDisabled'() {
        given:
        TestUtils.seedCluster(admin)

        when:
        def result = applySaved(planFile, deleteDisabled ? ['--no-delete'] : [])

        then:
        result.code == 0
        result.out == expectedReport(deleteDisabled ? "${planFile}-no-delete" : planFile)

        where:
        planFile                       | deleteDisabled
        'seed-topic-modification'      | true
        'seed-topic-modification'      | false
        'seed-topic-modification-3'    | true
        'seed-topic-modification-3'    | false
        'seed-acl-exists'              | false
        'no-changes'                   | false
        'no-changes-include-unchanged' | false
    }

    void 'test reading missing file throws ReadPlanInputException'() {
        when:
        def result = applyUnreadable(new File(directory, 'missing.json').path)

        then:
        result.code == 2
        result.out == expectedReport('missing-plan')
    }

    void 'test reading invalid file throws ReadPlanInputException'() {
        when:
        def result = applyUnreadable(TestUtils.getResourceFilePath('plans/invalid-plan.json'))

        then:
        result.code == 2
        result.out == expectedReport('invalid-plan')
    }

    private Map applySaved(String fixture, List<String> flags = []) {
        String path = TestUtils.getResourceFilePath("plans/${fixture}-plan.json")
        DesiredPlan plan = PlanTestSupport.mapper().readValue(new File(path), DesiredPlan)
        Map expected = TestUtils.snapshot(admin)
        Set<String> removed = new HashSet<>(expected.topics.keySet())
        if (!flags.contains('--skip-topics')) {
            plan.topicPlans.each { topic ->
                switch (topic.action) {
                    case ADD:
                        def details = topic.topicDetails.orElseThrow()
                        expected.topics[topic.name] = [partitions: details.partitions, replication: details.replication.orElseThrow(),
                                                      configs: details.configs]
                        break
                    case UPDATE:
                        def updated = expected.topics[topic.name]
                        topic.partitionPlan.ifPresent { updated.partitions = it.targetCount }
                        topic.replicationPlan.ifPresent { updated.replication = it.targetReplicationFactor }
                        topic.topicConfigPlans.each { config ->
                            if (config.action == REMOVE) updated.configs.remove(config.key)
                            if (config.action in [ADD, UPDATE]) updated.configs[config.key] = config.value.orElseThrow()
                        }
                        break
                    case REMOVE:
                        if (!flags.contains('--no-delete')) expected.topics.remove(topic.name)
                        break
                }
            }
        }
        if (!flags.contains('--skip-acls')) {
            plan.aclPlans.each { acl ->
                if (acl.action == ADD) expected.acls.add(acl.aclDetails.toAclBinding())
                if (acl.action == REMOVE && !flags.contains('--no-delete')) expected.acls.remove(acl.aclDetails.toAclBinding())
            }
        }
        def result = TestUtils.runCli(*(['-f', path] + flags + ['apply', '-p', path]))
        if (result.code == 0) {
            TestUtils.assertState(admin, expected)
            removed.removeAll(expected.topics.keySet())
            TestUtils.awaitDeletedTopics(admin, removed)
        }
        assert result.err.empty
        result
    }

    private Map applyUnreadable(String path) {
        Map before = TestUtils.snapshot(admin)
        def result = TestUtils.runCli('-f', TestUtils.getResourceFilePath('plans/simple.yaml'), 'apply', '-p', path)
        assert TestUtils.snapshot(admin) == before
        assert result.err.empty
        result
    }

    private static String expectedReport(String fixture) {
        APPLY_REPORTS[fixture].join('\n').replace(
                '[CONNECT] Preparing apply against Kafka target: localhost:9092',
                "[CONNECT] Preparing apply against Kafka target: ${System.getenv('KAFKA_BOOTSTRAP_SERVERS')}".toString()) + '\n'
    }
}
