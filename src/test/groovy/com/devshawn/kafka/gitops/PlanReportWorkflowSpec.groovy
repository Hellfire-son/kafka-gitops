// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import ch.qos.logback.classic.Logger
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.service.KafkaService
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.slf4j.LoggerFactory
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.TempDir

import static com.devshawn.kafka.gitops.PlanTestSupport.entry
import static com.devshawn.kafka.gitops.PlanTestSupport.mapper
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN

class PlanReportWorkflowSpec extends Specification {
    @TempDir File directory

    void 'all 145 topics with multiple partitions are read and counted without detailed blocks'() {
        given:
        Map<String, Integer> partitions = (1..145).collectEntries { [("example-${it}".toString()): 6] }
        def kafka = PlanReportKafka.withTopics(partitions)
        File output = new File(directory, 'plan.json')

        when:
        def result = plan(kafka, PlanReportKafka.topicState(partitions), [], ['-o', output.absolutePath] + flags)

        then:
        result.code == 0
        result.text.contains('[PLAN] No changes required.')
        !result.text.contains('Topic:')
        !result.text.contains('ordered assignments')
        result.text.contains('Topics: 0 to create, 0 to update, 0 to delete')
        result.text.contains('ACLs: 0 to create, 0 to delete')
        result.text.contains('Unchanged topics: 145\n')
        result.text.contains('Total resource changes: 0\n')
        result.text.contains('Status: NO_CHANGES')
        kafka.metadataReads == [partitions.keySet().toList()]
        kafka.rfReads.empty
        mapper().readTree(output).get('topicPlans').size() == exported

        where:
        flags                   | exported
        []                      | 0
        ['--include-unchanged']  | 145
    }

    void 'one creation among 144 unchanged topics has a three-line block and preserves logging at #flags'() {
        given:
        Map<String, Integer> partitions = (1..144).collectEntries { [(String.format('example-%03d', it)): it <= 93 ? 2 : 1] }
        def kafka = PlanReportKafka.withTopics(partitions)
        kafka.entries = [entry('retention.ms', '60000')]
        String desired = 'topics:\n' + partitions.collect { name, count ->
            "  ${name}:\n    partitions: ${count}\n    replication: 1\n    configs:\n      retention.ms: 60000\n"
        }.join() + '  example-new:\n    partitions: 1\n    replication: 1\n'

        when:
        def result = plan(kafka, desired, flags)
        def lines = result.text.readLines()
        int start = lines.indexOf('[PLAN] [CREATE] Topic: example-new')

        then:
        result.code == 0
        start >= 0
        lines.subList(start, lines.indexOf('[SUMMARY]')) == [
                '[PLAN] [CREATE] Topic: example-new', '  Partitions: 1', '  Replication factor: 1']
        lines.count { it.contains('exists, it will not be created.') } == info
        lines.count { it.contains('| [NO_CHANGE]') } == info
        result.text.contains('Topics: 1 to create, 0 to update, 0 to delete')
        result.text.contains('Unchanged topics: 144\n')
        result.text.contains('Total resource changes: 1\n')
        result.text.contains('Status: PLAN_BUILT')
        result.text.contains('Ready for apply: FRESH_PREFLIGHT_REQUIRED')
        !result.text.contains('Current ordered assignments')

        where:
        flags         | info
        []            | 0
        ['--verbose'] | 144
    }

    void 'creation renders target partitions, RF and configured values'() {
        given:
        def kafka = PlanReportKafka.withTopics([:])
        String desired = PlanReportKafka.topicState(['example-new': 4], 2) +
                '    configs:\n      cleanup.policy: compact\n      retention.ms: 3600000\n'

        when:
        def result = plan(kafka, desired)

        then:
        result.code == 0
        result.text.contains('''[PLAN] [CREATE] Topic: example-new
  Partitions: 4
  Replication factor: 2
  Config cleanup.policy: compact
  Config retention.ms: 3600000
''')
        !result.text.contains('ABSENT')
    }

    void 'a config-only update shows old and new values without an RF preview'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-config': 2])
        kafka.entries = [entry('retention.ms', '60000')]
        String desired = PlanReportKafka.topicState(['example-config': 2]) + '    configs:\n      retention.ms: 120000\n'

        when:
        def result = plan(kafka, desired)

        then:
        result.code == 0
        result.text.contains('[PLAN] [UPDATE] Topic: example-config\n  Config retention.ms: 60000 -> 120000 [UPDATE]\n')
        result.text.contains('Topics: 0 to create, 1 to update, 0 to delete')
        !result.text.contains('Partitions:')
        !result.text.contains('Replication factor:')
        kafka.rfReads.empty
    }

    void 'adding and removing overrides reports effective old value and reset to default once per topic'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-config': 1])
        kafka.entries = [entry('cleanup.policy', 'compact'),
                         entry('retention.ms', '604800000', false, ConfigEntry.ConfigSource.DEFAULT_CONFIG)]
        String desired = PlanReportKafka.topicState(['example-config': 1]) + '    configs:\n      retention.ms: 3600000\n'

        when:
        def result = plan(kafka, desired)

        then:
        result.code == 0
        result.text.contains('Config cleanup.policy: compact -> DEFAULT (remove override) [REMOVE]')
        result.text.contains('Config retention.ms: 604800000 -> 3600000 [ADD]')
        result.text.contains('Topics: 0 to create, 1 to update, 0 to delete')
        result.text.contains('Total resource changes: 1\n')
    }

    void 'partition increase renders the counts without replica assignments'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-partitions': 1])

        when:
        def result = plan(kafka, PlanReportKafka.topicState(['example-partitions': 3]))

        then:
        result.code == 0
        result.text.contains('[PLAN] [UPDATE] Topic: example-partitions\n  Partitions: 1 -> 3\n')
        !result.text.contains('Current ordered assignments')
        result.text.contains('Topics: 0 to create, 1 to update, 0 to delete')
        kafka.rfReads.empty
    }

    void 'mixed changes and no-delete count resources with their executable actions: #flags'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-update': 1, 'example-old': 1])
        kafka.topicEntries = ['example-update': [entry('retention.ms', '60000')]]
        kafka.acls = [PlanReportKafka.obsoleteAcl()]
        String desired = PlanReportKafka.topicState(['example-new': 1, 'example-update': 3]) +
                '    configs:\n      retention.ms: 120000\n' + PlanReportKafka.aclState()
        File output = new File(directory, 'mixed.json')

        when:
        def result = plan(kafka, desired, flags, ['-o', output.absolutePath])
        def exported = mapper().readValue(output, DesiredPlan)

        then:
        result.code == 0
        result.text.contains('[PLAN] [CREATE] Topic: example-new')
        result.text.contains('[PLAN] [UPDATE] Topic: example-update\n  Partitions: 1 -> 3\n  Config retention.ms: 60000 -> 120000 [UPDATE]')
        result.text.contains('[PLAN] [CREATE] ACL: example-user-0 (resource=example-new, operation=READ)')
        result.text.contains('[PLAN] [DELETE] Topic: example-old\n  Destructive operation: deletes topic data.') == (deleted == 1)
        result.text.contains('[PLAN] [DELETE] ACL: Unnamed ACL (resource=example-old, operation=WRITE)') == (deleted == 1)
        result.text.contains("Topics: 1 to create, 1 to update, ${deleted} to delete")
        result.text.contains("ACLs: 1 to create, ${deleted} to delete")
        result.text.contains("Total resource changes: ${3 + 2 * deleted}\n")
        exported.topicPlans*.action*.name() == (deleted == 1 ? ['ADD', 'UPDATE', 'REMOVE'] : ['ADD', 'UPDATE'])
        exported.aclPlans*.action*.name() == (deleted == 1 ? ['REMOVE', 'ADD'] : ['ADD'])
        exported.topicPlans.find { it.name == 'example-update' }.partitionPlan.get().targetCount == 3
        exported.topicPlans.find { it.name == 'example-update' }.topicConfigPlans.first().previousValue.get() == '60000'

        where:
        flags           | deleted
        []              | 1
        ['--no-delete'] | 0
    }

    void 'RF changes retain actual and proposed partition assignments and safety evidence: #before -> #target'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-rf': replicas.size()])
        kafka.replication = snapshot(['example-rf': replicas], [0: 'rack-a', 1: 'rack-b'])
        File output = new File(directory, 'rf.json')

        when:
        def result = plan(kafka, PlanReportKafka.topicState(['example-rf': replicas.size()], target), [], ['-o', output.absolutePath])
        def rf = mapper().readTree(output).get('topicPlans').get(0).get('replicationPlan')

        then:
        result.code == 0
        result.text.contains('[PLAN] [UPDATE] Topic: example-rf')
        result.text.contains("Replication factor: ${before} -> ${target}\n")
        result.text.contains('Current ordered assignments:')
        replicas.withIndex().every { ids, index ->
            result.text.contains("Partition ${index}: ${ids}; leader=0; ISR=${ids}, ELR=UNKNOWN, LastKnownELR=UNKNOWN")
        }
        result.text.contains('ELR feature: DISABLED')
        result.text.contains("Proposed ordered assignments (subject to fresh apply preflight):\n    Partition 0: ${proposed}")
        result.text.contains('DECREASE_PERMISSION_REQUIRED') == (target == 1)
        result.text.contains('Topics: 0 to create, 1 to update, 0 to delete')
        rf.get('schemaVersion').asInt() == 1
        rf.get('targetReplicationFactor').asInt() == target

        where:
        replicas        | before   | target | proposed
        [[0]]           | '1'      | 2      | [0, 1]
        [[0, 1]]        | '2'      | 1      | [0]
        [[0], [0, 1]]   | '[1, 2]' | 2      | [0, 1]
    }

    void 'blocked RF stays visible with proposed changes and preserves any existing JSON: #condition'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-rf': 1, 'example-unchanged': 1])
        kafka.replication = snapshot(['example-rf': [[0]], 'example-unchanged': [[0, 1]]], [0: 'rack-a', 1: 'rack-b'])
        kafka.elrMode = mode
        if (condition == 'insufficient brokers') kafka.replication = broker(kafka.replication, 1, [uncordoned: UNKNOWN])
        String desired = PlanReportKafka.topicState(['example-rf': 1, 'example-unchanged': 1] + extra, 2)
        File output = new File(directory, 'existing.json')
        output.text = 'fixture plan to preserve'

        when:
        def result = plan(kafka, desired, [], ['-o', output.absolutePath])

        then:
        result.code == 2
        result.text.contains('[PLAN] [BLOCKED] Topic: example-rf\n')
        result.text.contains('Replication factor: 1 -> 2')
        result.text.contains('Proposed assignments: NOT_CALCULATED')
        result.text.contains("[BLOCKED] ${reason}")
        result.text.contains("ELR feature: ${mode}")
        result.text.contains('ELR=UNKNOWN, LastKnownELR=UNKNOWN')
        !result.text.contains('Topic: example-unchanged')
        !result.text.contains('No changes required')
        result.text.contains('Unchanged topics: 1\n')
        result.text.contains('Blocked topics: 1\n')
        result.text.contains("Total resource changes: ${extra.size()} (proposed, plan blocked)")
        result.text.contains('Blocking diagnostics: 1\n')
        result.text.contains('Status: PLAN_BLOCKED')
        result.text.contains('Ready for apply: NO')
        result.text.contains('Executable JSON: NOT_WRITTEN')
        output.text == 'fixture plan to preserve'

        where:
        condition             | mode                                       | reason                 | extra
        'insufficient brokers'| ReplicationSafetyEvidence.ElrMode.DISABLED | 'INSUFFICIENT_BROKERS' | [:]
        'legacy ELR unknown'  | ReplicationSafetyEvidence.ElrMode.UNKNOWN  | 'ELR_MODE_UNKNOWN'     | ['example-new': 1]
    }

    void 'two RF blockers on one topic count once and exclude it from unchanged topics: #extraConfig'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-rf': 2])
        kafka.replication = snapshot(['example-rf': [[0], [0]]], [0: 'rack-a', 1: 'rack-b'])
        kafka.elrMode = ReplicationSafetyEvidence.ElrMode.UNKNOWN
        String desired = PlanReportKafka.topicState(['example-rf': 2], 2) + extraConfig

        when:
        def result = plan(kafka, desired)

        then:
        result.code == 2
        result.text.contains('[BLOCKED] ELR_MODE_UNKNOWN Resource: example-rf/partition-0')
        result.text.contains('[BLOCKED] ELR_MODE_UNKNOWN Resource: example-rf/partition-1')
        result.text.contains('Unchanged topics: 0\n')
        result.text.contains('Blocked topics: 1\n')
        result.text.contains('Blocking diagnostics: 2\n')
        result.text.contains('Blocked topics with other proposals: 1\n') == (proposed == 1)
        result.text.contains("Total resource changes: ${proposed} (proposed, plan blocked)")
        result.text.contains('Status: PLAN_BLOCKED')

        where:
        extraConfig                                 | proposed
        ''                                          | 0
        '    configs:\n      retention.ms: 60000\n' | 1
    }

    void 'explicit RF opt-out reports drift even when there is no executable change'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-rf': 2])
        File output = new File(directory, 'rf-disabled.json')

        when:
        def result = plan(kafka, PlanReportKafka.topicState(['example-rf': 2], 2), [],
                ['--no-manage-replication', '-o', output.absolutePath])

        then:
        result.code == 0
        result.text.contains('[PLAN] [PREVIEW] Topic: example-rf')
        result.text.contains('[WARNING] RF_MANAGEMENT_DISABLED Resource: example-rf')
        result.text.contains('Status: NO_CHANGES')
        !result.text.contains('[PLAN] [UPDATE]')
        !result.text.contains('Proposed ordered assignments')
        mapper().readTree(output).get('topicPlans').size() == 0
        kafka.rfReads.empty
    }

    void 'skipped resources are labeled and never read: #flags'() {
        given:
        def kafka = PlanReportKafka.withTopics([:])
        String desired = PlanReportKafka.topicState(['example-new': 1]) + PlanReportKafka.aclState()

        when:
        def result = plan(kafka, desired, flags)

        then:
        result.code == 0
        result.text.contains('Topics: SKIPPED') == flags.contains('--skip-topics')
        result.text.contains('ACLs: SKIPPED') == flags.contains('--skip-acls')
        result.text.contains('Unchanged topics:') == !flags.contains('--skip-topics')
        result.text.contains("Total resource changes: ${2 - flags.size()}\n")
        kafka.aclReads == (flags.contains('--skip-acls') ? 0 : 1)
        kafka.metadataReads.empty == flags.contains('--skip-topics')

        where:
        flags << [['--skip-topics'], ['--skip-acls'], ['--skip-topics', '--skip-acls']]
    }

    void 'a global ACL failure does not mark a topic named ACLs as blocked'() {
        given:
        def kafka = new PlanReportKafka() {
            @Override List<AclBinding> getAcls() {
                throw new KafkaExecutionException('Read ACLs', 'ACL metadata unavailable', KafkaExecutionException.Code.AUTHORIZATION_FAILED)
            }
        }
        kafka.topics = [new TopicListing('ACLs', Uuid.ZERO_UUID, false)]

        when:
        def result = plan(kafka, PlanReportKafka.topicState(['ACLs': 1]))

        then:
        result.code == 2
        result.text.contains('[BLOCKED] AUTHORIZATION_FAILED Resource: ACLs')
        !result.text.contains('Topic: ACLs')
        result.text.contains('Unchanged topics: 1\n')
        result.text.contains('Blocked topics: 0\n')
        result.text.contains('Blocking diagnostics: 1\n')
    }

    void 'text diagnostics mask synthetic credential values with verbose=#verbose'() {
        given:
        def kafka = PlanReportKafka.withTopics(['example-sensitive': 2])
        kafka.entries = [entry('max.message.bytes', 'fixture-current-confidential', true)]
        List<String> flags = verbose ? ['--verbose'] : []
        String credentials = '''      sasl.jaas.config: fixture-jaas-confidential
      ssl.keystore.password: fixture-ssl-confidential
      sasl.oauthbearer.client.credentials.client.secret: fixture-oauth-confidential
      vendor.token: fixture-vendor-confidential
'''
        String desired = '    configs:\n      max.message.bytes: fixture-broker-confidential\n' + credentials
        File output = new File(directory, 'sensitive.json')
        output.text = 'fixture plan to preserve'

        when:
        def created = plan(kafka, PlanReportKafka.topicState(['example-new': 1]) + '    configs:\n' + credentials, flags + ['--no-delete'])
        def blocked = plan(kafka, PlanReportKafka.topicState(['example-sensitive': 1]) + desired, flags, ['-o', output.absolutePath])
        def refusedExport = plan(kafka, PlanReportKafka.topicState(['example-sensitive': 2]) + desired, flags, ['-o', output.absolutePath])

        then:
        created.code == 0
        created.text.contains('Config sasl.jaas.config: [redacted]\n')
        blocked.code == 2
        blocked.text.contains('[PLAN] [BLOCKED] Topic: example-sensitive')
        blocked.text.contains('Config max.message.bytes: [redacted] -> [redacted] [PREVIEW_ONLY]')
        refusedExport.code == 2
        refusedExport.text.contains('Config max.message.bytes: [redacted] -> [redacted] [UPDATE]')
        refusedExport.text.contains('Plan serialization refused')
        [created, blocked, refusedExport].every { !it.text.contains('fixture-') }
        output.text == 'fixture plan to preserve'

        where:
        verbose << [false, true]
    }

    private Map plan(PlanReportKafka kafka, String desired, List<String> flags = [], List<String> options = []) {
        File state = new File(directory, 'state.yaml')
        state.setText(desired, 'UTF-8')
        def result = PlanReportKafka.cli(state, kafka, flags + ['plan'] + options)
        assert !kafka.wroteToKafka()
        result.text = result.text.replace('\r\n', '\n')
        result
    }
}

class PlanReportKafka extends TestKafkaService {
    ReplicationClusterSnapshot replication
    ReplicationSafetyEvidence.ElrMode elrMode = ReplicationSafetyEvidence.ElrMode.DISABLED
    List<Set<String>> rfReads = []
    int aclReads
    int aclWrites

    static PlanReportKafka withTopics(Map<String, Integer> partitions) {
        new PlanReportKafka(topics: partitions.keySet().collect { new TopicListing(it, Uuid.ZERO_UUID, false) },
                partitionCounts: partitions)
    }

    static String topicState(Map<String, Integer> partitions, int rf = 1) {
        'topics:\n' + partitions.collect { name, count ->
            "  ${name}:\n    partitions: ${count}\n    replication: ${rf}\n"
        }.join()
    }

    static String aclState() {
        '''users:
  example-user:
    principal: User:example-user
customUserAcls:
  example-user:
    read-topic:
      name: example-new
      type: TOPIC
      pattern: LITERAL
      operation: READ
'''
    }

    static AclBinding obsoleteAcl() {
        new AclBinding(new ResourcePattern(ResourceType.TOPIC, 'example-old', PatternType.LITERAL),
                new AccessControlEntry('User:example-old', '*', AclOperation.WRITE, AclPermissionType.ALLOW))
    }

    static Map cli(File state, KafkaService kafka, List<String> arguments) {
        def bytes = new ByteArrayOutputStream()
        def output = new PrintStream(bytes, true, 'UTF-8')
        def writer = new PrintWriter(output, true)
        def previous = System.out
        def root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        def kafkaLogger = (Logger) LoggerFactory.getLogger('org.apache.kafka')
        def rootLevel = root.level
        def kafkaLevel = kafkaLogger.level
        def command = new CommandLine(new MainCommand({ cfg, parser -> new StateManager(cfg, parser, kafka) } as MainCommand.StateManagerFactory))
        (command.subcommands.values().toList() + [command]).each {
            it.setOut(writer)
            it.setErr(writer)
        }

        int code
        try {
            System.setOut(output)
            code = command.execute(*(['-f', state.absolutePath] + arguments))
        } finally {
            writer.flush()
            System.setOut(previous)
            root.level = rootLevel
            kafkaLogger.level = kafkaLevel
        }
        [code: code, text: bytes.toString('UTF-8')]
    }

    @Override protected Admin buildAdminClient() {
        throw new AssertionError('A real Kafka Admin client is forbidden in this fixture')
    }

    @Override Map<String, TopicDescription> describeTopics(List<String> names) {
        if (replication == null) return super.describeTopics(names)
        metadataReads.add(new ArrayList<>(names))
        names.collectEntries { name -> [(name): description(name, replication.topics()[name], replication.brokers())] }
    }

    @Override ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names) {
        rfReads.add(new TreeSet<>(names))
        def evidence = new ReplicationSafetyEvidence(OptionalLong.empty(), elrMode, UNKNOWN, [:], [:], [:])
        new ReplicationClusterSnapshot(replication.clusterId(), replication.brokers(), replication.topics(),
                replication.reassignments(), replication.reassignmentState(), replication.replicaCounts(), replication.loadState(), evidence)
    }

    @Override List<AclBinding> getAcls() {
        aclReads++
        super.getAcls()
    }

    @Override void createAcl(AclBinding acl) {
        aclWrites++
        super.createAcl(acl)
    }

    @Override boolean deleteAcl(AclBinding acl) {
        aclWrites++
        super.deleteAcl(acl)
    }

    boolean wroteToKafka() {
        aclWrites > 0 || !createdTopics.empty || !deletedTopics.empty || !operations.empty || !partitionChanges.empty
    }
}
