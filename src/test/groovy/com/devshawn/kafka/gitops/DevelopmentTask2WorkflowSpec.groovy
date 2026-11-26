// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.exception.PlanBlockedException
import com.devshawn.kafka.gitops.manager.PlanManager
import com.devshawn.kafka.gitops.service.ParserService
import org.apache.kafka.clients.admin.TopicListing
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.TempDir

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*

class DevelopmentTask2WorkflowSpec extends Specification {
    @TempDir File directory
    Task2WorkflowKafka kafka = new Task2WorkflowKafka()
    List<ManagerConfig> configurations = []
    boolean productionBinding

    File state(int rf = 2) {
        File file = new File(directory, 'state.yaml')
        file.text = "topics:\n  acl-test:\n    partitions: 4\n    replication: ${rf}\n    configs:\n      max.message.bytes: 10485880\n"
        file
    }

    Map run(List<String> args, int rf = 2) {
        def output = new ByteArrayOutputStream()
        def previous = System.out
        MainCommand.StateManagerFactory factory = { cfg,parser ->
            configurations.add(cfg)
            new StateManager(cfg,parser,kafka,null,productionBinding)
        } as MainCommand.StateManagerFactory
        def cli = new CommandLine(new MainCommand(factory))
        cli.out = new PrintWriter(output,true); cli.err = new PrintWriter(output,true)
        cli.subcommands.values().each { it.out=cli.out; it.err=cli.err }
        int code
        try { System.setOut(new PrintStream(output,true,'UTF-8')); code=cli.execute(*(['--file',state(rf).absolutePath]+args)) }
        finally { System.setOut(previous) }
        cli.out.flush(); cli.err.flush()
        cli.subcommands.values().each { it.out.flush(); it.err.flush() }
        [code:code,text:output.toString('UTF-8')]
    }

    void 'plan RF defaults and deprecated alias and explicit disable are read only: #flags'() {
        given:
        File out = new File(directory,'plan.json')

        when:
        def result = run(['plan','--output',out.absolutePath]+flags)
        def tree = mapper().readTree(out)

        then:
        result.code == 0
        configurations.last().manageReplicationRequested == managed
        tree.get('topicPlans').get(0).has('replicationPlan') == managed
        result.text.contains('1048588 -> 10485880')
        result.text.contains('Current ordered assignments')
        !managed || result.text.contains('Partition 0: [0, 1]')
        0 <= result.text.indexOf('[PLAN] [UPDATE] Topic: acl-test')
        result.text.indexOf('[PLAN] [UPDATE] Topic: acl-test') < result.text.indexOf('[SUMMARY]')
        kafka.operations.empty && kafka.createdTopics.empty && kafka.deletedTopics.empty && kafka.partitionChanges.empty

        where:
        flags | managed
        [] | true
        ['--manage-replication'] | true
        ['--no-manage-replication'] | false
        ['--replication-rack-policy','BROKER_ONLY'] | true
    }

    void 'conflicting RF CLI flags fail before the state manager is constructed: #command'() {
        expect:
        run([command,'--manage-replication','--no-manage-replication']).with {
            code == 2 && text.contains('RF_OPTION_CONFLICT')
        }
        configurations.empty
        kafka.reads == 0
        kafka.operations.empty

        where:
        command << ['plan','apply']
    }

    void 'decrease is visible without planning permission and is not permanently authorized by JSON: #extra'() {
        given:
        kafka.current = snapshot(['acl-test':[[0,1],[0,1],[0,1],[0,1]]],[0:'1',1:'2'])
        File out = new File(directory,'decrease.json')

        when:
        def planned = run(['plan','--output',out.absolutePath]+extra,1)
        def applied = run(['apply','--plan',out.absolutePath],1)

        then:
        planned.code == 0
        planned.text.contains('-> 1')
        planned.text.contains('DECREASE_PERMISSION_REQUIRED') || planned.text.contains('NO_DELETE')
        applied.code == 2
        applied.text.contains('DECREASE_PERMISSION_REQUIRED')
        !out.text.contains('allowDecrease')
        kafka.operations.empty

        where:
        extra << [[],['--allow-replication-decrease']]
    }

    void 'no-delete dominates independent apply decrease permission'() {
        given:
        kafka.current = snapshot(['acl-test':[[0,1],[0,1],[0,1],[0,1]]],[0:'1',1:'2'])
        File out = new File(directory,'decrease.json')
        assert run(['plan','--output',out.absolutePath],1).code == 0

        expect:
        run(['--no-delete','apply','--plan',out.absolutePath,'--allow-replication-decrease'],1).with {
            code == 2 && text.contains('NO_DELETE')
        }
        kafka.operations.empty
    }

    void 'RF JSON with explicit disable is rejected even when both categories are skipped'() {
        given:
        File out = new File(directory,'rf.json')
        assert run(['plan','--output',out.absolutePath]).code == 0
        int reads = kafka.reads

        when:
        def result = run(['--skip-topics','--skip-acls','apply','--plan',out.absolutePath,'--no-manage-replication'])

        then:
        result.code == 2
        result.text.contains('MANAGE_REPLICATION_REQUIRED')
        kafka.reads == reads
        kafka.operations.empty && kafka.partitionChanges.empty
    }

    void 'blocked preview retains current and desired changes but never overwrites an executable file'() {
        given:
        kafka.current = broker(kafka.current,1,[uncordoned:UNKNOWN])
        File out = new File(directory,'retained.json'); out.text='existing file stays intact'

        when:
        def result = run(['plan','--output',out.absolutePath])

        then:
        result.code == 2
        result.text.contains('Partitions: 4 -> 4')
        result.text.contains('Replication factor: 1 -> 2')
        result.text.contains('1048588 -> 10485880')
        result.text.contains('INSUFFICIENT_BROKERS')
        result.text.contains('Discovered=2, eligible=1')
        result.text.contains('Proposed assignments: NOT_CALCULATED')
        result.text.contains('Broker 1: EXCLUDED')
        0 <= result.text.indexOf('[PLAN] [UPDATE] Topic: acl-test')
        result.text.indexOf('[PLAN] [UPDATE] Topic: acl-test') < result.text.indexOf('[BLOCKED] INSUFFICIENT_BROKERS')
        result.text.contains('Ready for apply: NO')
        out.text == 'existing file stays intact'
        kafka.operations.empty
    }

    void 'blocked manager cannot export a partial plan through its regular JSON writer'() {
        given:
        kafka.current = broker(kafka.current,1,[storage:UNKNOWN])
        File file = new File(directory,'must-not-exist.json')
        def cfg = new ManagerConfig.Builder().mergeFrom(options(file)).setManageReplicationRequested(true).build()
        def manager = new PlanManager(cfg,kafka,mapper())
        def parsed = new StateManager(cfg,new ParserService(state()),kafka).getAndValidateStateFile()
        def desired = new com.devshawn.kafka.gitops.domain.state.DesiredState.Builder().putAllTopics(parsed.topics).build()
        def changes = new DesiredPlan.Builder()
        manager.planTopicsReported(desired,changes)

        when:
        manager.writePlanToFile(changes.build())

        then:
        thrown(PlanBlockedException)
        !file.exists()
        kafka.operations.empty
    }

    void 'help documents enabled-by-default replication and disabling it'() {
        when:
        def cli=new CommandLine(new MainCommand())
        def parsed=cli.parseArgs('plan','--help')
        def help=cli.subcommands['plan'].usageMessage

        then:
        parsed.subcommand().usageHelpRequested
        help.contains('--no-manage-replication')
        help.replaceAll(/\s+/,' ').contains('enabled by default')
        configurations.empty
    }

    void 'unknown or enabled ELR blocks JSON while retaining known topic and broker diagnostics: #mode'() {
        given:
        kafka.elrMode=mode
        File out=new File(directory,'elr-blocked.json')

        when:
        def result=run(['plan','--output',out.absolutePath])

        then:
        result.code==2
        result.text.contains(code)
        result.text.contains('Partitions: 4 -> 4')
        result.text.contains('1048588 -> 10485880')
        result.text.contains('Proposed assignments: NOT_CALCULATED')
        result.text.contains('Broker 1:')
        !out.exists()
        kafka.operations.empty

        where:
        mode | code
        com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN | 'ELR_MODE_UNKNOWN'
        com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED | 'ELR_UNSUPPORTED'
    }

    void 'an excluded original broker cannot remain in an exported target: #condition'() {
        given:
        kafka.current=broker(kafka.current,0,change)
        File out=new File(directory,'original-excluded.json')

        when:
        def result=run(['plan','--output',out.absolutePath])

        then:
        result.code==2
        result.text.contains('TARGET_BROKER_INELIGIBLE')
        result.text.contains('Excluded target brokers=[0]')
        result.text.contains('Broker 0: EXCLUDED')
        result.text.contains('Proposed assignments: NOT_CALCULATED')
        !out.exists()
        kafka.operations.empty

        where:
        condition | change
        'cordoned' | [uncordoned:UNAVAILABLE]
        'cordoning unknown' | [uncordoned:UNKNOWN]
        'fencing unknown' | [unfenced:UNKNOWN]
        'no space' | [minimumUsableBytes:OptionalLong.of(0)]
    }

    void 'default apply binding independently verifies RF before followup writes from saved plan=#saved'() {
        given:
        productionBinding=true
        kafka.admin = new com.devshawn.kafka.gitops.service.Task2ExecutionAdmin()
        File out = new File(directory,'rf.json')

        when:
        def planned=run(['plan','--output',out.absolutePath])

        then:
        planned.code==0
        kafka.opens==0
        kafka.operations.empty
        kafka.admin.writes==0

        when:
        def applied=run(saved ? ['apply','--plan',out.absolutePath] : ['apply'])

        then:
        applied.code==0
        applied.text.contains('Reassignment: COMPLETED')
        applied.text.contains('Final verification: PASSED')
        applied.text.contains('Status: APPLY_COMPLETED')
        applied.text.contains('DISK_RESERVE_NOT_PROVEN')
        kafka.opens==1
        kafka.admin.writes==1
        kafka.admin.calls.count('close')==1
        kafka.admin.state.topics()['acl-test'].partitions().every { it.replicas()==[0,1] && it.isr()==[0,1] as Set }
        !kafka.operations.empty

        where:
        saved << [true, false]
    }

    void 'default apply binding refuses unknown critical metadata before every fake write'() {
        given:
        productionBinding=true
        kafka.admin = new com.devshawn.kafka.gitops.service.Task2ExecutionAdmin()
        File out = new File(directory,'rf.json')
        assert run(['plan','--output',out.absolutePath]).code==0
        kafka.admin.feature=[failure:true] // No positive cordoning/ELR applicability proof remains.

        when:
        def applied=run(['apply','--plan',out.absolutePath])

        then:
        applied.code==2
        applied.text.contains('CORDONING_UNKNOWN')
        applied.text.contains('Actual: UNKNOWN')
        applied.text.contains('Status: APPLY_BLOCKED')
        applied.text.contains('Successful resource changes: 0')
        kafka.admin.writes==0
        kafka.operations.empty
        kafka.admin.calls.count('close')==1
    }
}

class Task2WorkflowKafka extends TestKafkaService {
    def current = snapshot(['acl-test':[[0],[0],[0],[0]]],[0:'1',1:'2'])
    int reads
    int opens
    def elrMode=com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.DISABLED
    com.devshawn.kafka.gitops.service.Task2ExecutionAdmin admin
    @Override protected org.apache.kafka.clients.admin.Admin buildAdminClient() {
        opens++
        assert admin != null : 'No real AdminClient may be constructed in this fixture'
        admin.admin
    }
    @Override List<TopicListing> getTopics() { current.topics().keySet().collect { new TopicListing(it,org.apache.kafka.common.Uuid.ZERO_UUID,false) } }
    @Override Map describeTopics(List<String> names) { names.collectEntries { [(it):description(it,current.topics()[it],current.brokers())] } }
    @Override Map describeConfigsForTopics(List<String> names) {
        names.collectEntries { [(new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,it)):
                new org.apache.kafka.clients.admin.Config([entry('max.message.bytes','1048588')])] }
    }
    @Override com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names) {
        reads++
        def evidence=new com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence(OptionalLong.empty(),elrMode,UNKNOWN,[:],[:],[:])
        new com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot(current.clusterId(),current.brokers(),current.topics(),
                current.reassignments(),current.reassignmentState(),current.replicaCounts(),current.loadState(),evidence)
    }
    @Override com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot readReplicationPreflightSnapshot(Set<String> names) { reads++;current }
}
