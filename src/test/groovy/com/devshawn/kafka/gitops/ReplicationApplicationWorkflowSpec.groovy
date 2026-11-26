// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import ch.qos.logback.classic.Logger
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.Admission
import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.domain.state.AclDetails
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import com.devshawn.kafka.gitops.service.*
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.slf4j.LoggerFactory
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.observed
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*

/** Explicitly allowlisted unit workflow. Every Admin call is a proxy fake and every legacy mutation is in memory. */
class ReplicationApplicationWorkflowSpec extends Specification {
    @TempDir File directory
    def rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    def kafkaLogger = (Logger) LoggerFactory.getLogger('org.apache.kafka')
    def oldRoot = rootLogger.level
    def oldKafka = kafkaLogger.level

    void cleanup() { rootLogger.level = oldRoot; kafkaLogger.level = oldKafka }

    private File json(ReplicationApplicationTestHarness h, boolean extras = false) {
        def builder = new DesiredPlan.Builder()
        h.request.operations().each { o -> builder.addTopicPlans(new TopicPlan.Builder().setName(o.topicName()).setAction(UPDATE)
                .setReplicationPlan(o.plan()).build()) }
        if (extras) {
            builder.addTopicPlans(new TopicPlan.Builder().setName('other-topic').setAction(UPDATE)
                    .setPartitionPlan(new TopicPartitionPlan.Builder().setAction(UPDATE).setPreviousCount(1).setTargetCount(2).build())
                    .addTopicConfigPlans(config('retention.ms',UPDATE,'2000','1000')).build())
            builder.addAclPlans(new AclPlan.Builder().setName('acl').setAction(ADD).setAclDetails(new AclDetails.Builder()
                    .setName('orders').setType('TOPIC').setPattern('LITERAL').setPrincipal('User:test').setHost('*')
                    .setOperation('READ').setPermission('ALLOW').build()).build())
        }
        File file = new File(directory,'plan.json')
        file.setText(mapper().writeValueAsString(builder.build()),'UTF-8')
        file
    }

    private Map runCli(ReplicationApplicationTestHarness h, File file, List flags = ['--manage-replication'],
                       List global = [], boolean integrated = true, PipelineKafka supplied = null,
                       ReplicationApplication suppliedApplication = null) {
        def kafka = supplied ?: new PipelineKafka(harness:h)
        def application = integrated ? (suppliedApplication ?: h.application()) : null
        MainCommand.StateManagerFactory factory = { cfg,parser -> new StateManager(cfg,parser,kafka,application) } as MainCommand.StateManagerFactory
        def cli = new CommandLine(new MainCommand(factory))
        def output = new ByteArrayOutputStream()
        def oldOut = System.out
        def oldErr = System.err
        int code
        try {
            def stream = new PrintStream(output,true,'UTF-8')
            System.setOut(stream); System.setErr(stream)
            cli.out = new PrintWriter(stream,true); cli.err = new PrintWriter(stream,true)
            code = cli.execute(*(['--verbose'] + global + ['apply'] + flags + ['--plan',file.absolutePath]))
        } finally { System.setOut(oldOut); System.setErr(oldErr) }
        [code:code,text:output.toString('UTF-8'),kafka:kafka]
    }

    @Unroll
    void 'JSON CLI workflow completes #previous -> #target before configs partitions and ACL'() {
        given:
        def h = new ReplicationApplicationTestHarness([orders:previous],[orders:target])

        when:
        def r = runCli(h,json(h,true),flags)

        then:
        r.code == 0
        r.text.contains('Status: APPLY_COMPLETED')
        r.text.contains('Reassignment: COMPLETED')
        r.text.contains('Final verification: PASSED')
        r.text.contains("Replication factor: ${previous.first().size()} -> ${target}")
        r.text.contains('Successful resource changes: 3')
        !r.text.contains('application remains unavailable')
        h.lastResult.state() == State.COMPLETED
        h.opens == 1
        h.fake.closes == 1
        h.events.indexOf('legacy-partitions') > h.events.lastIndexOf('rf-target')
        h.events.indexOf('legacy-config') > h.events.lastIndexOf('rf-target')
        h.events.indexOf('legacy-acl') > h.events.lastIndexOf('rf-target')
        h.events.indexOf('close') < h.events.indexOf('legacy-partitions')

        where:
        previous | target | flags
        [[0,1],[1,0]] | 3 | ['--manage-replication']
        [[0,1,2],[1,2,0]] | 2 | ['--manage-replication','--allow-replication-decrease']
    }

    @Unroll
    void 'CLI permission #caseName stops before opening a session'() {
        given:
        def h = new ReplicationApplicationTestHarness([orders:[[0,1,2]]],[orders:2])

        when:
        def r = runCli(h,json(h,true),flags,global)

        then:
        r.code == 2
        r.text.contains(expected)
        !r.text.contains('Status: APPLY_COMPLETED')
        r.text.contains(expected == 'RF_OPTION_CONFLICT' ? 'Status: APPLY_FAILED' : 'Status: APPLY_BLOCKED')
        r.text.contains('Successful resource changes: 0')
        h.opens == 0
        h.fake.writes.isEmpty()
        !h.events.any { it.startsWith('legacy-') }

        where:
        caseName | flags | global | expected
        'manage explicitly disabled' | ['--no-manage-replication'] | [] | 'MANAGE_REPLICATION_REQUIRED'
        'decrease permission absent' | ['--manage-replication'] | [] | 'DECREASE_PERMISSION_REQUIRED'
        'no-delete overrides both' | ['--manage-replication','--allow-replication-decrease'] | ['--no-delete'] | 'NO_DELETE'
        'allow with explicit disable' | ['--allow-replication-decrease','--no-manage-replication'] | [] | 'RF_OPTION_CONFLICT'
    }

    @Unroll
    void 'UNKNOWN or BLOCKED #caseName never reaches executor or any resource write'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.snapshotMutation = mutation
        h.evidence = evidence

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains(expected)
        !r.text.contains('Status: APPLY_COMPLETED')
        !r.text.contains('private-')
        h.executionCalls == 0
        h.fake.writes.isEmpty()
        !h.events.any { it.startsWith('legacy-') }
        h.opens == 1
        h.fake.closes == 1

        where:
        caseName | mutation | evidence | expected
        'fencing' | { broker(it,3,[unfenced:UNKNOWN]) } | [:] | 'BROKER_FENCING_UNKNOWN'
        'placement admission' | { it } | [admissions:[:]] | 'PLACEMENT_UNKNOWN'
        'disk full' | { broker(it,3,[minimumUsableBytes:OptionalLong.of(0)]) } | [:] | 'DISK_INSUFFICIENT'
        'disk capacity' | { it } | [admissions:[3:new Admission(VERIFIED,UNKNOWN)]] | 'DISK_CAPACITY_UNKNOWN'
        'ELR' | { it } | [elrMode:ReplicationSafetyEvidence.ElrMode.UNKNOWN] | 'ELR_MODE_UNKNOWN'
        'cluster identity' | { changed(it,[clusterId:'private-cluster']) } | [:] | 'CLUSTER_ID_CHANGED'
        'topic identity' | { topic(it,'orders',[topicId:'private-id']) } | [:] | 'TOPIC_ID_CHANGED'
        'ISR lost' | { topic(it,'orders',[partitions:[new Partition(0,[0,1],OptionalInt.of(0),[0] as Set),it.topics().orders.partitions().last()]]) } | [:] | 'ISR_INCOMPLETE'
        'broker topology' | { broker(it,3,[rack:Optional.of('different-rack')]) } | [:] | 'TOPOLOGY_CHANGED'
        'active conflict' | { changed(it,[reassignments:[new TopicPartition('orders',0)] as Set]) } | [:] | 'ACTIVE_REASSIGNMENT'
        'config stability' | { it } | [configsStable:[orders:UNAVAILABLE]] | 'CONFIG_CHANGED'
    }

    void 'partial acceptance retains partition results and never applies subsequent resources'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        // One packet with one success and one rejection.
        h.batchSize = 2
        h.fake.errors[new TopicPartition('orders',1)] = new org.apache.kafka.common.errors.ClusterAuthorizationException('private-auth-response')

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        !r.text.contains('Status: APPLY_COMPLETED')
        !r.text.contains('Status: NO_CHANGES')
        r.text.contains('Status: APPLY_PARTIAL')
        r.text.contains('Partially applied resources: 1')
        r.text.contains('Resources not run: 2')
        r.text.contains('Successful resource changes: 0')
        r.text.contains('Requests may have been accepted')
        !r.text.contains('private-')
        h.failure instanceof ReplicationExecutionException
        h.failure.result.partiallyCompleted()
        h.failure.result.partitions()[new TopicPartition('orders',0)].state() == State.COMPLETED
        h.failure.result.partitions()[new TopicPartition('orders',1)].state() == State.FAILED
        !h.events.any { it.startsWith('legacy-') }
        h.fake.closes == 1
    }

    @Unroll
    void 'incomplete executor return #state is not CLI success'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.executeOverride = { engine,request ->
            def values = h.targets.collectEntries { key,target -> [(key):new PartitionOutcome(state,error)] }
            new ExecutionResult(values.keySet(),values)
        }

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('state=' + state)
        !r.text.contains('Status: APPLY_COMPLETED')
        !h.events.any { it.startsWith('legacy-') }
        h.fake.writes.isEmpty()
        h.fake.closes == 1

        where:
        state | error
        State.ACCEPTED | ErrorCategory.NONE
        State.RUNNING | ErrorCategory.NONE
        State.UNKNOWN | ErrorCategory.RPC_TIMEOUT
        State.FAILED | ErrorCategory.AUTHORIZATION
    }

    void 'unverified COMPLETED response is rejected by an independent terminal read'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.executeOverride = { engine,request ->
            def values = h.targets.collectEntries { key,target -> [(key):new PartitionOutcome(State.COMPLETED,ErrorCategory.NONE)] }
            new ExecutionResult(values.keySet(),values)
        }

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('phase=VERIFICATION')
        r.text.contains('state=UNKNOWN')
        r.text.contains('Final verification: UNCONFIRMED')
        r.text.contains('Status: APPLY_UNKNOWN')
        !r.text.contains('Status: APPLY_COMPLETED')
        !h.events.any { it.startsWith('legacy-') }
        h.fake.closes == 1
    }

    void 'timeout and rerun read observed target without resubmitting earlier work'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.fake.autoComplete = false
        File file = json(h)

        when:
        def first = runCli(h,file)
        h.targets.each { key,target -> h.fake.finish(key,target) }
        def second = runCli(h,file)

        then:
        first.code == 2
        first.text.contains('DEADLINE_EXCEEDED')
        first.text.contains('Status: APPLY_UNKNOWN')
        first.text.contains('Resources with unknown outcomes: 1')
        second.code == 0
        second.text.contains('Status: APPLY_COMPLETED')
        second.text.contains('Resources already satisfied: 1')
        second.text.contains('Successful resource changes: 0')
        h.lastResult.state() == State.COMPLETED
        h.lastResult.partitions().values().every { it.submission() == Submission.OBSERVED }
        h.fake.writes.size() == 1
        h.opens == 2
        h.fake.closes == 2
    }

    void 'old config-only JSON has no implicit RF or RF session'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        File file = new File(directory,'old.json')
        file.text = '{"topicPlans":[{"name":"orders","action":"UPDATE","topicConfigPlans":[{"key":"retention.ms","action":"UPDATE","value":"2000"}]}]}'

        when:
        def r = runCli(h,file,[])

        then:
        r.code == 0
        h.opens == 0
        h.fake.writes.isEmpty()
        h.events == ['legacy-config']
    }

    void 'default production construction still stops at EXECUTION_NOT_IMPLEMENTED for synthetic READY'() {
        given:
        def h = new ReplicationApplicationTestHarness()

        when:
        def r = runCli(h,json(h),['--manage-replication'],[],false)

        then:
        r.code == 2
        r.text.contains('RF execution not implemented')
        h.opens == 0
        h.fake.writes.isEmpty()
    }

    void 'real 3_9 metadata reader cannot acquire READY through the integration seam'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        def application = h.applicationWithRealReader()

        when:
        def r = runCli(h,json(h,true),['--manage-replication'],[],true,null,application)

        then:
        r.code == 2
        r.text.contains('CORDONING_UNKNOWN')
        !r.text.contains('private-')
        h.executionCalls == 0
        !h.events.any { it.startsWith('legacy-') }
        !h.realReaderAdmin.calls.contains('alterPartitionReassignments')
        h.realReaderAdmin.calls.count('close') == 1
        h.realReaderAdmin.calls.count('describeFeatures') == 2
    }

    @Unroll
    void 'invalid saved JSON #caseName is rejected before any metadata or mutation'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        File file = json(h,true)
        def tree = mapper().readValue(file,Map)
        mutation(tree)
        file.setText(mapper().writeValueAsString(tree),'UTF-8')

        when:
        def r = runCli(h,file)

        then:
        r.code == 2
        !r.text.contains('Status: APPLY_COMPLETED')
        h.opens == 0
        h.fake.writes.isEmpty()
        !h.events.any { it.startsWith('legacy-') }

        where:
        caseName | mutation
        'unknown RF schema' | { it.topicPlans[0].replicationPlan.schemaVersion = 99 }
        'unknown RF operation' | { it.topicPlans[0].replicationPlan.action = 'PRIVATE_UNKNOWN_OPERATION' }
        'same topic partition conflict' | { it.topicPlans[0].partitionPlan = [action:'UPDATE',previousCount:2,targetCount:3] }
    }

    void 'stale separate partition operation stops RF before its session is opened'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        def kafka = new PipelineKafka(harness:h)
        kafka.partitionCounts['other-topic'] = 4

        when:
        def r = runCli(h,json(h,true),['--manage-replication'],[],true,kafka)

        then:
        r.code == 2
        r.text.contains('Partition plan is stale')
        r.text.contains('Status: APPLY_BLOCKED')
        r.text.contains('Blocked resources: 1')
        h.opens == 0
        h.fake.writes.isEmpty()
        !h.events.any { it.startsWith('legacy-') }
    }

    @Unroll
    void 'lost or malformed executor return #caseName is UNKNOWN with possible submission'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.executeOverride = override

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('state=UNKNOWN')
        r.text.contains('Requests may have been accepted')
        !r.text.contains('Status: NO_CHANGES')
        !r.text.contains('private-')
        h.failure.result.partitions().values().every { it.submission() == Submission.UNKNOWN }
        h.fake.closes == 1
        !h.events.any { it.startsWith('legacy-') }

        where:
        caseName | override
        'null response' | { engine,request -> null }
        'wrong result scope' | { engine,request ->
            def key = new TopicPartition('different-topic',0)
            new ExecutionResult([key] as Set,[(key):new PartitionOutcome(State.COMPLETED,ErrorCategory.NONE)])
        }
        'failure after executor completed' | { engine,request ->
            assert engine.execute(request).state() == State.COMPLETED
            throw new IllegalStateException('private-lost-response')
        }
    }

    @Unroll
    void 'submission transport error #caseName is safe and never retried blindly'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.fake.submitFailure = failure

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        // An unacknowledged submission is observed until the shared deadline, never resubmitted.
        r.text.contains('DEADLINE_EXCEEDED')
        r.text.contains('Requests may have been accepted')
        !r.text.contains('private-')
        !r.text.contains('Status: APPLY_COMPLETED')
        h.fake.writes.size() == 1
        h.fake.closes == 1
        !h.events.any { it.startsWith('legacy-') }

        where:
        caseName | failure
        'connection lost' | new org.apache.kafka.common.errors.NetworkException('private-network-response')
        'RPC timeout' | new org.apache.kafka.common.errors.TimeoutException('private-rpc-response')
    }

    void 'preflight read failure closes once and does not expose metadata or write resources'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.snapshotMutation = { throw new IllegalStateException('private-metadata-response') }
        h.fake.failClose = true

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('METADATA_UNAVAILABLE')
        !r.text.contains('private-')
        !r.text.contains('SESSION_CLOSE')
        h.executionCalls == 0
        h.fake.closes == 1
        h.fake.writes.isEmpty()
        !h.events.any { it.startsWith('legacy-') }
    }

    void 'all RF topics pass batch preflight before any earlier topic or ACL can be changed'() {
        given:
        def h = new ReplicationApplicationTestHarness([orders:[[0,1]],payments:[[1,2]]],[orders:3,payments:3])
        h.evidence = [configsStable:[orders:VERIFIED,payments:UNAVAILABLE]]

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('CONFIG_CHANGED')
        r.text.contains('Blocked resources: 1')
        r.text.contains('Resources not run: 3')
        h.executionCalls == 0
        h.fake.writes.isEmpty()
        h.fake.closes == 1
        !h.events.any { it.startsWith('legacy-') }
    }

    void 'skip ACLs preserves completed RF and other topic operations'() {
        given:
        def h = new ReplicationApplicationTestHarness()

        when:
        def r = runCli(h,json(h,true),['--manage-replication'],['--skip-acls'])

        then:
        r.code == 0
        h.lastResult.state() == State.COMPLETED
        h.events.containsAll(['legacy-config','legacy-partitions'])
        !h.events.contains('legacy-acl')
    }

    void 'skipping both resource categories never opens RF Admin or writes anything'() {
        given:
        def h = new ReplicationApplicationTestHarness()

        when:
        def r = runCli(h,json(h,true),['--manage-replication'],['--skip-topics','--skip-acls'])

        then:
        r.code == 0
        h.opens == 0
        h.fake.writes.isEmpty()
        h.events.isEmpty()
    }

    void 'legacy resource failure after completed RF never claims that nothing changed or leaks the error'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        def kafka = new PipelineKafka(harness:h,failLegacy:true)

        when:
        def r = runCli(h,json(h,true),['--manage-replication'],[],true,kafka)

        then:
        r.code == 2
        r.text.contains('phase=FOLLOWUP')
        r.text.contains('state=COMPLETED')
        r.text.contains('Status: APPLY_PARTIAL')
        r.text.contains('Successful resource changes: 1')
        r.text.contains('Partially applied resources: 1')
        !r.text.contains('Status: APPLY_COMPLETED')
        !r.text.contains('Status: NO_CHANGES')
        !r.text.contains('private-')
        !h.fake.writes.isEmpty()
    }

    void 'late partition drift after completed RF keeps its typed reason and stops the other operations'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        def kafka = new PipelineKafka() {
            @Override boolean increaseTopicPartitions(String name, int previous, int target) {
                throw new KafkaExecutionException('Partition plan is stale.', "Topic ${name} has 3 partitions, expected ${previous}.",
                        KafkaExecutionException.Code.PARTITION_PLAN_STALE, KafkaExecutionException.WriteOutcome.NOT_SENT)
            }
        }
        kafka.harness = h

        when:
        def r = runCli(h, json(h, true), ['--manage-replication'], [], true, kafka)

        then:
        r.code == 2
        r.text.contains('phase=FOLLOWUP')
        r.text.contains('PARTITION_PLAN_STALE: Partition plan is stale.')
        r.text.contains('Topic other-topic has 3 partitions, expected 1.')
        r.text.contains('Result: BLOCKED')
        r.text.contains('Status: APPLY_PARTIAL')
        r.text.contains('Successful resource changes: 1')
        r.text.contains('Resources not run: 1')
        !h.events.any { it.startsWith('legacy-') }
        !h.fake.writes.isEmpty()
    }

    @Unroll
    void 'APPLY reports a terminal removed ISR as #status while preserving submission and masking'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2], [1, 2, 3]], 2)
        def h = trial.harness
        if (failure) {
            trial.afterExecution { complete ->
                h.fake.current = trial.extraIsr(complete)
                h.snapshotMutation = { ignored -> throw new ClusterAuthorizationException('private-response-token') }
            }
        } else {
            trial.waitThenConverge(reads)
        }

        when:
        def r = runCli(h, json(h), ['--allow-replication-decrease'])

        then:
        r.code == code
        r.text.contains("Status: APPLY_${status}")
        r.text.contains("Final verification: ${code == 0 ? 'PASSED' : 'UNCONFIRMED'}")
        r.text.contains("Successful resource changes: ${code == 0 ? 1 : 0}")
        if (code != 0) {
            assert r.text.contains('Resources with unknown outcomes: 1')
            assert r.text.contains('submission=SENT, last known=COMPLETED')
            assert r.text.contains(reason)
        }
        !r.text.contains('private-response-token')
        h.fake.writes.size() == 1

        where:
        failure | reads             | status      | code | reason
        false   | 2                 | 'COMPLETED' | 0    | ''
        false   | Integer.MAX_VALUE | 'UNKNOWN'   | 2    | 'DEADLINE_EXCEEDED'
        true    | 0                 | 'UNKNOWN'   | 2    | 'AUTHORIZATION'
    }

    void 'a saved RF decrease without original replicas is refused before opening Admin'() {
        given:
        def h = new ReplicationApplicationTestHarness([orders: [[0, 1, 2]]], [orders: 2])
        File file = json(h)
        Map saved = mapper().readValue(file, Map)
        saved.topicPlans.first().replicationPlan.assignments.first().remove('previousReplicas')
        mapper().writeValue(file, saved)

        when:
        def r = runCli(h, file, ['--allow-replication-decrease'])

        then:
        r.code == 2
        h.opens == 0
        h.fake.writes.empty
    }

    void 'session close failure after RF completion remains a safe non-success result'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.fake.failClose = true

        when:
        def r = runCli(h,json(h,true))

        then:
        r.code == 2
        r.text.contains('phase=SESSION_CLOSE')
        r.text.contains('Final verification: PASSED')
        r.text.contains('Status: APPLY_PARTIAL')
        r.text.contains('Successful resource changes: 1')
        !r.text.contains('private-')
        !r.text.contains('Status: APPLY_COMPLETED')
        !h.events.any { it.startsWith('legacy-') }
        h.fake.closes == 1
    }

    void 'interrupt restores flag stops followup writes and closes the session once'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.fake.autoComplete = false
        h.clock.interruptPause = true

        when:
        def r = runCli(h,json(h,true))

        then:
        Thread.currentThread().isInterrupted()
        r.code == 2
        r.text.contains('INTERRUPTED')
        !h.events.any { it.startsWith('legacy-') }
        h.fake.closes == 1

        cleanup:
        Thread.interrupted()
    }
}

class PipelineKafka extends TestKafkaService {
    ReplicationApplicationTestHarness harness
    boolean failLegacy

    PipelineKafka() {
        topics = [new TopicListing('other-topic', org.apache.kafka.common.Uuid.ZERO_UUID, false)]
        partitionCounts['other-topic'] = 1
    }
    @Override com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot readReplicationPreflightSnapshot(Set<String> names) {
        observed(harness.fake.current)
    }
    @Override boolean increaseTopicPartitions(String name,int previous,int target) {
        harness.events.add('legacy-partitions'); super.increaseTopicPartitions(name,previous,target)
    }
    @Override void updateTopicConfig(Map configs) {
        harness.events.add('legacy-config')
        if (failLegacy) throw new KafkaExecutionException('private-wrapper','private-server-response')
        super.updateTopicConfig(configs)
    }
    @Override void createAcl(org.apache.kafka.common.acl.AclBinding acl) {
        harness.events.add('legacy-acl'); super.createAcl(acl)
    }
}
