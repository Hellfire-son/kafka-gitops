// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.service.KafkaService
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification
import spock.lang.TempDir

import java.time.Duration

import static com.devshawn.kafka.gitops.PlanReportKafka.*
import static com.devshawn.kafka.gitops.PlanTestSupport.entry
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.snapshot

class ApplyReportWorkflowSpec extends Specification {
    @TempDir File directory

    void 'the same creation is proposed by PLAN and confirmed by APPLY from #source'() {
        given:
        Map<String, Integer> existing = (1..144).collectEntries { [("example-${it}".toString()): 2] }
        def kafka = withTopics(existing)
        File state = writeState(topicState(existing + ['example-new': 1]))
        File saved = new File(directory, 'plan.json')
        def proposal = run(state, kafka, ['plan', '-o', saved.absolutePath])
        byte[] originalJson = saved.bytes
        assert proposal.code == 0 && !kafka.wroteToKafka()

        when:
        def result = run(state, kafka, ['apply'] + (source == 'JSON' ? ['-p', saved.absolutePath] : []))

        then:
        result.code == 0
        proposal.text.contains('[PLAN] [CREATE] Topic: example-new\n  Partitions: 1\n  Replication factor: 1\n')
        proposal.text.contains('Unchanged topics: 144\n')
        !proposal.text.contains('Result: SUCCESS')
        result.text.contains('[APPLY] [CREATE] Topic: example-new\n  Partitions: 1\n  Replication factor: 1\n  Result: SUCCESS\n')
        result.text.readLines().count { it.startsWith('[APPLY] [') } == 1
        result.text.contains('Topics: 1 created, 0 updated, 0 deleted\n')
        result.text.contains("Unchanged topics: ${unchanged}\n")
        result.text.contains('Successful resource changes: 1\n')
        result.text.contains('Failed resource changes: 0\n')
        result.text.contains('Status: APPLY_COMPLETED')
        !result.text.contains('\u001b[')
        saved.bytes == originalJson
        kafka.createdTopics.size() == 1

        where:
        source          | unchanged
        'desired state' | '144'
        'JSON'          | 'UNKNOWN'
    }

    void 'unchanged resources produce no operation blocks with verbose=#verbose'() {
        given:
        def kafka = withTopics(['example-unchanged': 2])
        kafka.entries = [entry('retention.ms', '60000')]
        File state = writeState(topicState(['example-unchanged': 2]) + '    configs:\n      retention.ms: 60000\n')

        when:
        def result = run(state, kafka, (verbose ? ['--verbose'] : []) + ['apply'])

        then:
        result.code == 0
        result.text.contains('[APPLY] No changes required.')
        !result.text.contains('Topic: example-unchanged')
        result.text.contains('exists, it will not be created.') == verbose
        result.text.contains('| [NO_CHANGE] retention.ms') == verbose
        result.text.contains('Unchanged topics: 1\n')
        result.text.contains('Successful resource changes: 0\n')
        result.text.contains('Status: NO_CHANGES')
        !kafka.wroteToKafka()

        where:
        verbose << [false, true]
    }

    void 'an unchanged entry in saved JSON does not establish the cluster total'() {
        given:
        def kafka = withTopics(['example-unchanged': 1])
        File state = writeState(topicState(['example-unchanged': 1]))
        File saved = new File(directory, 'unchanged.json')
        assert run(state, kafka, ['plan', '--include-unchanged', '-o', saved.absolutePath]).code == 0
        int reads = kafka.metadataReads.size()

        when:
        def result = run(state, kafka, ['apply', '-p', saved.absolutePath])

        then:
        result.code == 0
        result.text.contains('[APPLY] No changes required by the saved plan.')
        result.text.contains('Unchanged topics: UNKNOWN\n')
        result.text.contains('Unchanged topics listed in JSON: 1\n')
        result.text.contains('Status: NO_CHANGES')
        kafka.metadataReads.size() == reads
        !kafka.wroteToKafka()
    }

    void 'multiple verified changes count each topic once and identify both ACL rules: #flags'() {
        given:
        def kafka = withTopics(['example-config': 1, 'example-update': 1, 'example-old': 1])
        kafka.topicEntries = [
                'example-config': [entry('cleanup.policy', 'compact'), entry('retention.ms', '604800000', false, ConfigEntry.ConfigSource.DEFAULT_CONFIG)],
                'example-update': [entry('retention.ms', '60000')]
        ]
        kafka.acls = [obsoleteAcl()]
        File state = writeState('''topics:
  example-new:
    partitions: 1
    replication: 1
  example-config:
    partitions: 1
    replication: 1
    configs:
      retention.ms: 3600000
  example-update:
    partitions: 3
    replication: 1
    configs:
      retention.ms: 120000
''' + aclState())
        File saved = new File(directory, 'mixed.json')
        assert run(state, kafka, ['plan', '-o', saved.absolutePath]).code == 0

        when:
        def result = run(state, kafka, flags + ['apply', '-p', saved.absolutePath])

        then:
        result.code == 0
        result.text.contains('[APPLY] [UPDATE] Topic: example-update\n  Partitions: 1 -> 3 [SUCCESS]\n  Config retention.ms: 60000 -> 120000 [UPDATE] [SUCCESS]\n  Result: SUCCESS')
        result.text.contains('Config cleanup.policy: compact -> DEFAULT (remove override) [REMOVE] [SUCCESS]')
        result.text.contains('Config retention.ms: UNKNOWN -> 3600000 [ADD] [SUCCESS]')
        result.text.contains('[APPLY] [CREATE] ACL: example-user-0 (resource=example-new, operation=READ)\n  Resource type: TOPIC, pattern: LITERAL\n  Principal: User:example-user, host: *, permission: ALLOW\n  Result: SUCCESS')
        result.text.contains('[APPLY] [DELETE] Topic: example-old') == (deletions == 1)
        result.text.contains('[APPLY] [DELETE] ACL: Unnamed ACL (resource=example-old, operation=WRITE)\n  Resource type: TOPIC, pattern: LITERAL\n  Principal: User:example-old, host: *, permission: ALLOW\n  Result: SUCCESS') == (deletions == 1)
        result.text.contains("Topics: 1 created, 2 updated, ${deletions} deleted\n")
        result.text.contains("ACLs: 1 created, ${deletions} deleted\n")
        result.text.contains("Successful resource changes: ${4 + deletions * 2}\n")
        kafka.partitionChanges*.name == ['example-update']
        kafka.operations*.operation*.opType()*.name() == ['DELETE', 'SET', 'SET']
        kafka.deletedTopics == (deletions == 1 ? ['example-old'] : [])
        kafka.aclWrites == 1 + deletions

        where:
        flags           | deletions
        []              | 1
        ['--no-delete'] | 0
    }

    void 'skipped scopes never contribute applied resources: #flags'() {
        given:
        def kafka = withTopics([:])
        File state = writeState(topicState(['example-new': 1]) + aclState())

        when:
        def result = run(state, kafka, flags + ['apply'])

        then:
        result.code == 0
        result.text.contains('Topics: SKIPPED') == flags.contains('--skip-topics')
        result.text.contains('ACLs: SKIPPED') == flags.contains('--skip-acls')
        result.text.contains("Successful resource changes: ${2 - flags.size()}\n")
        kafka.createdTopics.size() == (flags.contains('--skip-topics') ? 0 : 1)
        kafka.aclWrites == (flags.contains('--skip-acls') ? 0 : 1)
        flags.size() != 2 || result.text.contains('Status: NO_CHANGES')

        where:
        flags << [['--skip-topics'], ['--skip-acls'], ['--skip-acls', '--skip-topics']]
    }

    void 'a saved partition plan reports verified no-op or preflight drift without writes: #actual'() {
        given:
        def kafka = withTopics(['example-partitions': 1])
        File state = writeState(topicState(['example-new': 1, 'example-partitions': 3]))
        File saved = new File(directory, 'partitions.json')
        assert run(state, kafka, ['plan', '-o', saved.absolutePath]).code == 0
        kafka.partitionCounts['example-partitions'] = actual

        when:
        def result = run(state, kafka, ['apply', '-p', saved.absolutePath])

        then:
        result.code == code
        result.text.contains('[APPLY] [UPDATE] Topic: example-partitions\n  Partitions: 1 -> 3\n  Result: ' + outcome)
        result.text.contains('Status: ' + status)
        result.text.contains('Topics: 0 created, 0 updated, 0 deleted') == (code == 2)
        result.text.contains('Partition plan is stale.') == (code == 2)
        result.text.contains('[APPLY] [CREATE] Topic: example-new\n  Result: NOT_RUN') == (code == 2)
        kafka.partitionChanges.empty
        kafka.createdTopics.size() == (code == 2 ? 0 : 1)

        where:
        actual | outcome   | status            | code
        2      | 'BLOCKED' | 'APPLY_BLOCKED'   | 2
        3      | 'NO_OP'   | 'APPLY_COMPLETED' | 0
    }

    void 'an RF blocker in desired state has one resource outcome and stops unrelated proposed writes'() {
        given:
        def kafka = withTopics(['example-rf': 2])
        kafka.replication = snapshot(['example-rf': [[0], [0]]], [0: 'rack-a', 1: 'rack-b'])
        kafka.elrMode = ReplicationSafetyEvidence.ElrMode.UNKNOWN
        File state = writeState(topicState(['example-rf': 2, 'example-new': 1], 2))

        when:
        def result = run(state, kafka, ['apply'])

        then:
        result.code == 2
        result.text.contains('ELR_MODE_UNKNOWN Resource: example-rf/partition-0')
        result.text.contains('ELR_MODE_UNKNOWN Resource: example-rf/partition-1')
        result.text.contains('[APPLY] [BLOCKED] Topic: example-rf')
        result.text.contains('[APPLY] [CREATE] Topic: example-new\n  Result: NOT_RUN')
        result.text.contains('Unchanged topics: 0\n')
        result.text.contains('Blocked resources: 1\n')
        result.text.contains('Resources not run: 1\n')
        result.text.contains('Successful resource changes: 0\n')
        result.text.contains('Status: APPLY_BLOCKED')
        !kafka.wroteToKafka()
    }

    void 'real KafkaService distinguishes a response rejection, timeout and confirmed write before close failure: #outcome'() {
        given:
        def admin = Mock(Admin)
        def config = new KafkaGitopsConfig.Builder().build()
        def kafka = new KafkaService(config) {
            @Override protected Admin buildAdminClient() { admin }
        }
        File state = writeState('topics: {}\n')
        File saved = new File(directory, 'create.json')
        saved.text = '''{"topicPlans":[
          {"name":"example-new","action":"ADD","topicDetails":{"partitions":1,"replication":1}},
          {"name":"example-pending","action":"ADD","topicDetails":{"partitions":1,"replication":1}}
        ]}'''
        def future = new KafkaFutureImpl<Void>()
        if (failure == null) future.complete(null)
        else future.completeExceptionally(failure)
        def response = new ApplyCreateTopicsResponse(future)

        when:
        def result = run(state, kafka, ['apply', '-p', saved.absolutePath])

        then:
        1 * admin.createTopics({ it*.name() == ['example-new'] }) >> response
        1 * admin.close(Duration.ZERO) >> { if (failure == null) throw new IllegalStateException('fixture-close-error') }
        0 * admin._
        result.code == 2
        result.text.contains('  Result: ' + outcome + '\n')
        result.text.contains('[APPLY] [CREATE] Topic: example-pending\n  Result: NOT_RUN\n')
        result.text.contains("Successful resource changes: ${outcome == 'SUCCESS' ? 1 : 0}\n")
        result.text.contains(summary)
        result.text.contains('Status: ' + status)
        !result.text.contains('fixture-')

        where:
        failure                                              | outcome   | status          | summary
        new InvalidConfigurationException('fixture-invalid') | 'FAILED'  | 'APPLY_FAILED'  | 'Failed resource changes: 1'
        new TimeoutException('fixture-timeout')              | 'UNKNOWN' | 'APPLY_UNKNOWN' | 'Resources with unknown outcomes: 1'
        null                                                 | 'SUCCESS' | 'APPLY_PARTIAL' | 'Resources not run: 1'
    }

    void 'a failed later config does not make a partially updated topic successful'() {
        given:
        def kafka = new PlanReportKafka() {
            @Override void updateTopicConfig(Map configs) {
                String key = configs.values().first().first().configEntry().name()
                if (key == 'max.message.bytes') throw new KafkaExecutionException('Synthetic config rejected.', 'Invalid configuration.',
                        KafkaExecutionException.Code.KAFKA_ERROR, KafkaExecutionException.WriteOutcome.REJECTED)
                super.updateTopicConfig(configs)
            }
        }
        File state = writeState('''topics:
  test-topic:
    partitions: 3
    replication: 1
    configs:
      cleanup.policy: compact
      max.message.bytes: 2048
      retention.ms: 60000
  z-pending:
    partitions: 1
    replication: 1
''' + aclState())

        when:
        def result = run(state, kafka, ['apply'])

        then:
        result.code == 2
        result.text.contains('Partitions: 1 -> 3 [SUCCESS]')
        result.text.contains('Config cleanup.policy: UNKNOWN -> compact [ADD] [SUCCESS]')
        result.text.contains('Config max.message.bytes: UNKNOWN -> 2048 [ADD] [FAILED]')
        result.text.contains('Config retention.ms: UNKNOWN -> 60000 [ADD] [NOT_RUN]')
        result.text.contains('  Result: PARTIAL')
        result.text.contains('Topics: 0 created, 0 updated, 0 deleted')
        result.text.contains('Successful resource changes: 0\n')
        result.text.contains('Partially applied resources: 1\n')
        result.text.contains('Resources not run: 2\n')
        result.text.contains('Status: APPLY_PARTIAL')
        kafka.partitionChanges*.name == ['test-topic']
        kafka.operations*.operation*.configEntry()*.name() == ['cleanup.policy']
        kafka.createdTopics.empty && kafka.aclWrites == 0
    }

    void 'desired apply retains masking of known and unrecognized sensitive config values with verbose=#verbose'() {
        given:
        def kafka = withTopics(['example-sensitive': 1])
        kafka.entries = [entry('max.message.bytes', 'fixture-old-confidential', true)]
        File state = writeState(topicState(['example-sensitive': 1]) + '''    configs:
      max.message.bytes: fixture-broker-confidential
      sasl.jaas.config: fixture-jaas-confidential
      ssl.keystore.password: fixture-ssl-confidential
      vendor.token: fixture-token-confidential
''')

        when:
        def result = run(state, kafka, (verbose ? ['--verbose'] : []) + ['apply'])

        then:
        result.code == 0
        result.text.contains('Config max.message.bytes: [redacted] -> [redacted] [UPDATE] [SUCCESS]')
        result.text.contains('Config sasl.jaas.config: [redacted] -> [redacted] [ADD] [SUCCESS]')
        !result.text.contains('fixture-')
        result.text.contains('Successful resource changes: 1\n')
        kafka.operations.size() == 4

        where:
        verbose << [false, true]
    }

    private File writeState(String yaml) {
        File state = new File(directory, 'state.yaml')
        state.setText(yaml, 'UTF-8')
        state
    }

    private static Map run(File state, KafkaService kafka, List<String> arguments) {
        def result = PlanReportKafka.cli(state, kafka, arguments)
        result.text = result.text.replace('\r\n', '\n')
        result
    }
}

class ApplyCreateTopicsResponse extends CreateTopicsResult {
    private final KafkaFuture<Void> future

    ApplyCreateTopicsResponse(KafkaFuture<Void> future) {
        super([:])
        this.future = future
    }

    @Override KafkaFuture<Void> all() { future }
}
