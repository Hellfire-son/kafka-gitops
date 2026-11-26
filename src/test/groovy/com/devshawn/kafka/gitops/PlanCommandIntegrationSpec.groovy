package com.devshawn.kafka.gitops

import org.apache.kafka.clients.admin.Admin
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

@Unroll
class PlanCommandIntegrationSpec extends Specification {
    private static final String HEADER = 'Generating execution plan...\n\n[CONFIG] Loading Kafka properties ...\n'
    private static final String VALIDATION_ERROR = '[ERROR] STATE_VALIDATION_FAILED; Resource: plan; Actual: INVALID_OR_INCOMPLETE; Required: valid input and all mandatory preconditions; State: BLOCKED.\n' +
            '  Action: resolve the reported condition, inspect Kafka state after any apply attempt, and re-plan.\n'

    @Shared Admin admin
    @TempDir File directory

    void setupSpec() {
        admin = TestUtils.openFixtureAdmin()
        TestUtils.cleanUpCluster(admin)
    }

    void cleanupSpec() {
        TestUtils.closeFixtureAdmin(admin)
    }

    void 'test various valid plans - #planName'() {
        given:
        File output = new File(directory, 'plan.json')

        when:
        def result = runPlan('-f', state(planName), 'plan', '-o', output.path)

        then:
        result.code == 0
        assertPlan(planName, output)

        where:
        planName << [
                'simple',
                'application-service',
                'kafka-connect-service',
                'kafka-streams-service',
                'topics-and-services',
                'multi-file',
                'simple-users',
                'custom-service-acls',
                'custom-user-acls',
                'custom-group-id-application',
                'custom-group-id-connect',
                'custom-application-id-streams',
                'custom-storage-topic',
                'custom-storage-topics',
                'default-replication',
                'default-replication-multiple',
                'describe-topic-acl-disabled',
                'describe-topic-acl-enabled'
        ]
    }

    void 'test skip-acls flag'() {
        given:
        File output = new File(directory, 'plan.json')

        when:
        def result = runPlan('-f', state(planName), '--skip-acls', 'plan', '-o', output.path)

        then:
        result.code == 0
        result.out.contains('  ACLs: SKIPPED\n')
        assertPlan(planName, output)

        where:
        planName << ['skip-acls']
    }

    void 'test skip-topics flag'() {
        given:
        File output = new File(directory, 'plan.json')

        when:
        def result = runPlan('-f', state(planName), '--skip-topics', 'plan', '-o', output.path)

        then:
        result.code == 0
        result.out.contains('  Topics: SKIPPED\n')
        assertPlan(planName, output)

        where:
        planName << ['skip-topics']
    }

    void 'test various valid plans with seed - #planName'() {
        given:
        seed()
        File output = new File(directory, 'plan.json')
        def args = ['-f', state(planName)] + (deleteDisabled ? ['--no-delete'] : []) + ['plan', '-o', output.path]

        when:
        def result = runPlan(*args)

        then:
        result.code == (blocked ? 2 : 0)
        if (blocked) {
            assert result.out.contains('[BLOCKED] TOPIC_CHANGE_UNSAFE Resource: topic-with-configs-2\n')
            assert result.out.contains('Reason: Decreasing partitions is not supported (topic: topic-with-configs-2, 6 -> 3).\n')
            assert result.out.contains('  Status: PLAN_BLOCKED\n')
            assert result.out.contains('  Executable JSON: NOT_WRITTEN')
            assert !output.exists()
        } else {
            assertPlan(planName, output)
        }

        where:
        planName                            | deleteDisabled | blocked
        'seed-topic-modification'           | false          | false
        'seed-topic-modification-2'         | false          | false
        'seed-topic-modification-3'         | false          | true
        'seed-topic-modification-no-delete' | true           | false
        'seed-acl-exists'                   | true           | false
        'seed-exclude-topics'               | false          | false
        'seed-exclude-include-topics'       | false          | false
        'seed-include-topics'               | false          | false
    }

    void 'test include unchanged flag - #planName #includeUnchanged'() {
        given:
        seed()
        File output = new File(directory, 'plan.json')
        def args = ['-f', state(planName), 'plan', '-o', output.path] + (includeUnchanged ? ['--include-unchanged'] : [])
        String expected = includeUnchanged ? "${planName}-include-unchanged" : planName

        when:
        def result = runPlan(*args)

        then:
        result.code == 0
        assertPlan(expected, output)

        where:
        planName     | includeUnchanged
        'seed-basic' | false
        'seed-basic' | true
    }

    void 'test invalid plans - #planName'() {
        given:
        File output = new File(directory, 'plan.json')

        when:
        def result = runPlan('-f', state(planName), 'plan', '-o', output.path)

        then:
        result.code == 2
        result.out == HEADER + diagnostic
        !output.exists()

        where:
        planName                        | diagnostic
        'invalid-missing-principal'     | missingConfiguration('Not set: [principal] for service: test-service')
        'invalid-topic'                 | invalid('Missing or invalid required fields in state file.')
        'unrecognized-property'        | invalid('Unrecognized field in state file.')
        'invalid-format'                | invalid('Invalid field format in state file.')
        'invalid-missing-user-principal'| missingConfiguration('Missing principal for user test-user')
        'invalid-storage-topics'        | invalid('Unrecognized field in state file.')
        'invalid-default-replication-1' | invalid('The default replication factor must be a positive integer in 1..32767.')
        'invalid-default-replication-2' | invalid('Not set: [replication] in state file definition: topics -> test-topic')
    }

    void 'test file that does not exist'() {
        when:
        def result = runPlan('-f', new File(directory, 'missing.yaml').path, 'plan')

        then:
        result.code == 2
        result.out == HEADER + invalid('The specified state file could not be found.')
    }

    void 'test writing file throws a WritePlanOutputException'() {
        given:
        File parent = new File(directory, 'regular-file')
        parent.setText('sentinel', 'UTF-8')
        File output = new File(parent, 'plan.json')

        when:
        def result = runPlan('-f', state('simple'), 'plan', '-o', output.path)

        then:
        result.code == 2
        result.out.contains('  Status: PLAN_BUILT\n')
        result.out.endsWith('[ERROR] Error writing execution plan to file: The execution plan file could not be written. Check file access.\n')
        parent.getText('UTF-8') == 'sentinel'
        !output.exists()
    }

    void 'test plan that has no changes - #includeUnchanged'() {
        given:
        seed()
        File output = new File(directory, 'plan.json')
        def args = ['-f', state(planName), 'plan', '-o', output.path] + (includeUnchanged ? ['--include-unchanged'] : [])
        String expected = includeUnchanged ? "${planName}-include-unchanged" : planName

        when:
        def result = runPlan(*args)

        then:
        result.code == 0
        result.out == HEADER + "[CONNECT] Connecting to Kafka target: ${System.getenv('KAFKA_BOOTSTRAP_SERVERS')} ...\n" + '''[METADATA] Reading Kafka resource and broker metadata ...
[PLAN] Calculating and summarizing changes ...
[PLAN] No changes required.
[SUMMARY]
  Topics: 0 to create, 0 to update, 0 to delete
  ACLs: 0 to create, 0 to delete
  Unchanged topics: 4
  Total resource changes: 0
  Status: NO_CHANGES
  Ready for apply: FRESH_PREFLIGHT_REQUIRED
'''
        assertPlan(expected, output)

        where:
        planName     | includeUnchanged
        'no-changes' | false
        'no-changes' | true
    }

    private void seed() {
        TestUtils.cleanUpCluster(admin)
        TestUtils.seedCluster(admin)
    }

    private Map runPlan(String... args) {
        Map before = TestUtils.snapshot(admin)
        def result = TestUtils.runCli(args)
        assert TestUtils.snapshot(admin) == before
        assert result.err.empty
        result
    }

    private static String state(String name) {
        TestUtils.getResourceFilePath("plans/${name}.yaml")
    }

    private static boolean assertPlan(String fixture, File output) {
        def mapper = PlanTestSupport.mapper()
        Map expected = mapper.readValue(TestUtils.getResourceFileContent("plans/${fixture}-plan.json"), Map)
        // Legacy fixtures are also executable APPLY input. PLAN orders desired topics by name,
        // then the removals by name, with configs ordered by key.
        expected.topicPlans.sort { left, right ->
            ((left.action == 'REMOVE') <=> (right.action == 'REMOVE')) ?: (left.name <=> right.name)
        }
        expected.topicPlans.each { it.topicConfigPlans.sort { config -> config.key } }
        JSONAssert.assertEquals(mapper.writeValueAsString(expected), output.getText('UTF-8'), true)
        true
    }

    private static String invalid(String message) {
        "[INVALID] ${message}\n" + VALIDATION_ERROR
    }

    private static String missingConfiguration(String message) {
        "[ERROR] Missing required configuration: ${message}\n\n[ERROR] An error has occurred during the planning process. No plan was created.\n"
    }
}
