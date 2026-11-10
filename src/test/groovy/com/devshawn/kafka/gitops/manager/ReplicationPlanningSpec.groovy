package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.MainCommand
import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.state.DesiredState
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.util.LogUtil
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.TopicListing
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.*

class ReplicationPlanningSpec extends Specification {
    @TempDir File directory
    def kafka = new PlanningKafka()

    private ManagerConfig settings(boolean manage = true, boolean decrease = false, boolean noDelete = false) {
        new ManagerConfig.Builder().mergeFrom(options(null,noDelete)).setManageReplicationRequested(manage)
                .setAllowReplicationDecreaseRequested(decrease).build()
    }

    private static DesiredState state(int rf = 2, int partitions = 2, Map<String,String> configs = [:]) {
        new DesiredState.Builder().putTopics('orders',new TopicDetails.Builder().setPartitions(partitions)
                .setReplication(rf).putAllConfigs(configs).build()).build()
    }

    @Unroll
    void 'opt-in=#manage controls RF planning without changing legacy warning'() {
        given:
        def builder = new DesiredPlan.Builder()

        when:
        String output = PartitionPlanSpec.capture {
            new PlanManager(settings(manage), kafka, mapper()).planTopics(state(),builder)
        }
        def topic = builder.build().topicPlans.first()

        then:
        topic.replicationPlan.present == manage
        topic.action == (manage ? UPDATE : NO_CHANGE)
        kafka.snapshotReads.size() == (manage ? 1 : 0)
        manage ? output.contains('preview only') : output.contains('replicas will not be changed')
        kafka.operations.isEmpty()
        kafka.partitionChanges.isEmpty()

        where:
        manage << [false,true]
    }

    void 'generated plan round trips ordered assignments and previous config values then apply is blocked'() {
        given:
        kafka.entries = [entry('retention.ms','1000')]
        File file = new File(directory,'rf-plan.json')
        def config = new ManagerConfig.Builder().mergeFrom(settings()).setPlanFile(file).build()
        def manager = new PlanManager(config,kafka,mapper())
        def builder = new DesiredPlan.Builder()

        when:
        manager.planTopics(state(2,2,['retention.ms':'2000']),builder)
        def original = builder.build().toChangesOnlyPlan()
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()

        then:
        original == restored
        restored.topicPlans.first().replicationPlan.get().assignments*.previousReplicas == [[0],[1]]
        restored.topicPlans.first().topicConfigPlans.first().previousValue.get() == '1000'
        !file.text.contains('allowReplicationDecrease')
        !file.text.contains('sasl.jaas.config')

        when:
        new ApplyManager(config,kafka).applyTopics(restored)

        then:
        thrown(ValidationException)
        kafka.operations.isEmpty()
        kafka.createdTopics.isEmpty()
        kafka.deletedTopics.isEmpty()
        kafka.partitionChanges.isEmpty()
    }

    void 'RF and partition growth are rejected before requesting RF snapshot'() {
        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(state(2,3),new DesiredPlan.Builder())

        then:
        def error = thrown(ValidationException)
        error.message.contains('cannot be combined')
        kafka.snapshotReads.isEmpty()
    }

    void 'unchanged RF and new topic preserve previous semantics without RF metadata'() {
        given:
        def builder = new DesiredPlan.Builder()
        def desired = new DesiredState.Builder().mergeFrom(state(1)).putTopics('new-topic',
                new TopicDetails.Builder().setPartitions(3).setReplication(3).build()).build()

        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(desired,builder)

        then:
        kafka.snapshotReads.isEmpty()
        builder.build().topicPlans.every { it.replicationPlan.empty }
        builder.build().topicPlans.find { it.name == 'orders' }.action == NO_CHANGE
        builder.build().topicPlans.find { it.name == 'new-topic' }.action == ADD
    }

    @Unroll
    void 'decrease flag=#allow no-delete=#noDelete enforced during planning'() {
        given:
        kafka.rfSnapshot = snapshot([orders:[[0,1,2],[1,2,0]]])
        kafka.initial = kafka.rfSnapshot

        when:
        new PlanManager(settings(true,allow,noDelete),kafka,mapper()).planTopics(state(),new DesiredPlan.Builder())

        then:
        def error = thrown(ValidationException)
        error.message.contains(noDelete ? '--no-delete' : '--allow-replication-decrease')

        where:
        allow | noDelete
        false | false
        true | true
        false | true
    }

    void 'explicit decrease displays each partition permission and preflight warning'() {
        given:
        kafka.rfSnapshot = snapshot([orders:[[0,1,2],[1,2,0]]])
        kafka.initial = kafka.rfSnapshot
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(settings(true,true),kafka,mapper()).planTopics(state(),builder)
        String text = PartitionPlanSpec.capture { LogUtil.printPlan(builder.build(),false,false,false) }

        then:
        text.contains('~ replication: 3 -> 2')
        text.contains('partition 0: [0, 1, 2] -> [0, 1]')
        text.contains('independent apply permission')
        text.contains('preflight')
        text.contains('application remains unavailable')
    }

    void 'heterogeneous RF display uses partition assignments instead of fictional previous RF'() {
        given:
        kafka.rfSnapshot = snapshot([orders:[[0,1],[1,2,3]]])
        kafka.initial = kafka.rfSnapshot
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(state(3),builder)
        String text = PartitionPlanSpec.capture { LogUtil.printPlan(builder.build(),false,false,false) }

        then:
        text.contains('replication: mixed [2, 3] -> 3')
        text.contains('partition 0: [0, 1] -> [0, 1, 2]')
        text.contains('partition 1: [1, 2, 3] -> [1, 2, 3]')
    }

    void 'BROKER_ONLY is explicit and warns about rack failure protection'() {
        given:
        kafka.rfSnapshot = snapshot([orders:[[0],[1]]],[0:null,1:null,2:null])
        kafka.initial = kafka.rfSnapshot
        def config = new ManagerConfig.Builder().mergeFrom(settings()).setReplicationRackPolicy(BROKER_ONLY).build()
        def builder = new DesiredPlan.Builder()

        when:
        String text = PartitionPlanSpec.capture { new PlanManager(config,kafka,mapper()).planTopics(state(),builder) }

        then:
        text.contains('rack failure protection is not established')
        builder.build().topicPlans.first().replicationPlan.get().rackPolicy == BROKER_ONLY
    }

    @Unroll
    void 'RF cannot accompany safety-critical config #key'() {
        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(state(2,2,[(key):'2']),new DesiredPlan.Builder())

        then:
        def error = thrown(ValidationException)
        error.message.contains('Use separate plans')

        where:
        key << ['min.insync.replicas','leader.replication.throttled.replicas','follower.replication.throttled.replicas']
    }

    void 'drift between initial descriptions and RF snapshot cannot generate stale plan'() {
        given:
        kafka.rfSnapshot = snapshot([orders:[[0],[2]]])

        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(state(),new DesiredPlan.Builder())

        then:
        def error = thrown(ValidationException)
        error.message.contains('metadata changed')
    }

    void 'skip-topics bypasses all topic and RF reads'() {
        given:
        def config = new ManagerConfig.Builder().mergeFrom(settings()).setSkipTopicsDisabled(true).build()

        when:
        new PlanManager(config,null,mapper()).planTopics(state(),new DesiredPlan.Builder())

        then:
        noExceptionThrown()
    }

    void 'decrease flag without opt-in fails before any Kafka access'() {
        when:
        new PlanManager(settings(false,true),null,mapper()).planTopics(state(),new DesiredPlan.Builder())

        then:
        thrown(ValidationException)
    }

    @Unroll
    void 'RF CLI options parse for plan and preflight while rack override remains plan-only: #flag'() {
        given:
        def cli = new CommandLine(new MainCommand())
        cli.out = new PrintWriter(new StringWriter())
        cli.err = new PrintWriter(new StringWriter())

        when:
        def parsed = cli.parseArgs(*(['plan','--manage-replication'] + (flag == ['--manage-replication'] ? [] : flag)))

        then:
        parsed.subcommand().hasMatchedOption('--manage-replication')

        expect:
        if (flag.first() == '--replication-rack-policy') {
            try { cli.parseArgs('apply', *flag); assert false }
            catch (CommandLine.UnmatchedArgumentException expected) { }
        } else {
            cli.parseArgs('apply', *flag).subcommand().hasMatchedOption(flag.first())
        }

        where:
        flag << [['--manage-replication'],['--allow-replication-decrease'],['--replication-rack-policy','BROKER_ONLY']]
    }

    void 'verbose plan output with RF keeps sensitive current and desired configs redacted'() {
        given:
        def builder = new DesiredPlan.Builder()
        kafka.entries = [entry('sasl.jaas.config','private-log-old',true)]

        when:
        new PlanManager(settings(),kafka,mapper()).planTopics(state(2,2,['sasl.jaas.config':'private-log-new']),builder)
        String text = PartitionPlanSpec.capture { LogUtil.printPlan(builder.build(),false,false,false) }

        then:
        !text.contains('private-log')
        text.contains('[redacted]')
    }

    @Unroll
    void 'invalid RF CLI combination fails locally #arguments'() {
        given:
        def cli = new CommandLine(new MainCommand())
        cli.out = new PrintWriter(new StringWriter())
        cli.err = new PrintWriter(new StringWriter())

        expect:
        cli.execute(*arguments) == 2

        where:
        arguments << [['plan','--allow-replication-decrease'],['plan','--replication-rack-policy','BROKER_ONLY']]
    }

    void 'multiple topics and config-map iteration yield reproducible full JSON'() {
        given:
        kafka.rfSnapshot = snapshot([zeta:[[0]],alpha:[[1]]])
        kafka.initial = kafka.rfSnapshot
        def config = settings()
        def make = { boolean reverse ->
            def builder = new DesiredState.Builder()
            (reverse ? ['zeta','alpha'] : ['alpha','zeta']).each { name ->
                builder.putTopics(name,new TopicDetails.Builder().setPartitions(1).setReplication(2)
                        .putConfigs('retention.ms','2000').putConfigs('segment.bytes','1000').build())
            }
            def planned = new DesiredPlan.Builder()
            new PlanManager(config,kafka,mapper()).planTopics(builder.build(),planned)
            mapper().writeValueAsString(planned.build())
        }

        expect:
        make(false) == make(true)
    }
}

class PlanningKafka extends TestKafkaService {
    ReplicationClusterSnapshot rfSnapshot = snapshot()
    ReplicationClusterSnapshot initial = rfSnapshot
    List<Set<String>> snapshotReads = []

    @Override List<TopicListing> getTopics() { initial.topics().keySet().collect { new TopicListing(it,false) } }

    @Override Map<String,TopicDescription> describeTopics(List<String> names) {
        names.collectEntries { [(it):description(it,initial.topics()[it],initial.brokers())] }
    }

    @Override ReplicationClusterSnapshot readReplicationSnapshot(Set<String> names) {
        snapshotReads.add(new TreeSet<>(names))
        rfSnapshot
    }

    @Override ReplicationClusterSnapshot readReplicationPreflightSnapshot(Set<String> names) {
        snapshotReads.add(new TreeSet<>(names))
        rfSnapshot // No execution evidence: old planning snapshots must remain BLOCKED.
    }
}
