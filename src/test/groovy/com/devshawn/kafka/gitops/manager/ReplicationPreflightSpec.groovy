// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.MainCommand
import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.Admission
import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.domain.state.AclDetails
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.service.KafkaService
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartition
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Reason.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class ReplicationPreflightSpec extends Specification {
    private static ManagerConfig settings(boolean manage = true, boolean decrease = true, boolean noDelete = false) {
        new ManagerConfig.Builder().mergeFrom(options(null,noDelete)).setManageReplicationRequested(manage)
                .setAllowReplicationDecreaseRequested(decrease).build()
    }

    static DesiredPlan combined(TopicReplicationPlan rf) {
        new DesiredPlan.Builder().addTopicPlans(
                new TopicPlan.Builder().setName('create-before-rf').setAction(ADD)
                        .setTopicDetails(new TopicDetails.Builder().setPartitions(1).setReplication(1).build()).build(),
                new TopicPlan.Builder().setName('configs-before-rf').setAction(UPDATE)
                        .addTopicConfigPlans(config('retention.ms',UPDATE,'2000','1000')).build(),
                new TopicPlan.Builder().setName('delete-before-rf').setAction(REMOVE).build(),
                new TopicPlan.Builder().setName('partitions-before-rf').setAction(UPDATE)
                        .setPartitionPlan(new TopicPartitionPlan.Builder().setAction(UPDATE).setPreviousCount(1).setTargetCount(2).build()).build(),
                new TopicPlan.Builder().setName('orders').setAction(UPDATE).setReplicationPlan(rf).build()).build()
    }

    @Unroll
    void 'BLOCKED #caseName prevents every Kafka write including preceding operations'() {
        given:
        def f = fixture()
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ observed(mutation(f.base),evidence) })

        when:
        new ApplyManager(settings(),kafka).applyTopics(combined(f.plan))

        then:
        def error = thrown(ReplicationPreflightException)
        error.outcome == ReplicationPreflightException.Outcome.BLOCKED
        error.results.orders.reasons()*.code().contains(code)
        !error.message.contains('private-')
        kafka.writes == 0
        kafka.reads == 1

        where:
        caseName | mutation | evidence | code
        'cluster changed' | { changed(it,[clusterId:'private-cluster']) } | [:] | CLUSTER_ID_CHANGED
        'topic removed' | { changed(it,[topics:[:]]) } | [:] | TOPIC_NOT_FOUND
        'topic recreated' | { topic(it,'orders',[topicId:'private-id']) } | [:] | TOPIC_ID_CHANGED
        'partitions changed' | { topic(it,'orders',[partitions:it.topics().orders.partitions().take(1)]) } | [:] | PARTITION_COUNT_CHANGED
        'replicas changed' | { topic(it,'orders',[partitions:[new Partition(0,[2],OptionalInt.of(2),[2] as Set),it.topics().orders.partitions().last()]]) } | [:] | PREVIOUS_REPLICAS_CHANGED
        'leader missing' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.empty(),[0] as Set),it.topics().orders.partitions().last()]]) } | [:] | LEADER_UNKNOWN
        'ISR incomplete' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.of(0),[] as Set),it.topics().orders.partitions().last()]]) } | [:] | ISR_INCOMPLETE
        'min ISR missing' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.empty()]) } | [:] | MIN_ISR_UNKNOWN
        'min ISR changed' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(2)]) } | [:] | MIN_ISR_CHANGED
        'RF below min ISR' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(3)]) } | [:] | TARGET_BELOW_MIN_ISR
        'reassignment active' | { changed(it,[reassignments:[new TopicPartition('orders',0)] as Set]) } | [:] | ACTIVE_REASSIGNMENT
        'broker offline' | { broker(it,2,[reachable:UNAVAILABLE]) } | [:] | BROKER_OFFLINE
        'broker health unknown' | { broker(it,2,[reachable:UNKNOWN]) } | [:] | BROKER_HEALTH_UNKNOWN
        'fencing unknown' | { broker(it,2,[unfenced:UNKNOWN]) } | [:] | BROKER_FENCING_UNKNOWN
        'cordoned broker' | { broker(it,2,[uncordoned:UNAVAILABLE]) } | [:] | BROKER_CORDONED
        'cordoning unknown' | { broker(it,2,[uncordoned:UNKNOWN]) } | [:] | CORDONING_UNKNOWN
        'racks incomplete' | { broker(it,2,[rack:Optional.empty()]) } | [:] | RACK_METADATA_UNKNOWN
        'disk insufficient' | { broker(it,2,[minimumUsableBytes:OptionalLong.of(0)]) } | [:] | DISK_INSUFFICIENT
        'usable bytes unknown' | { broker(it,2,[minimumUsableBytes:OptionalLong.empty()]) } | [:] | DISK_CAPACITY_UNKNOWN
        'required reserve unknown' | { it } | [admissions:[2:new Admission(VERIFIED,UNKNOWN)]] | DISK_CAPACITY_UNKNOWN
        'placement forbidden' | { it } | [admissions:[2:new Admission(UNAVAILABLE,VERIFIED)]] | PLACEMENT_FORBIDDEN
        'placement unknown' | { it } | [admissions:[:]] | PLACEMENT_UNKNOWN
        'ELR mode unknown' | { it } | [elrMode:com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN] | ELR_MODE_UNKNOWN
        'ELR enabled' | { it } | [elrMode:com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED] | ELR_UNSUPPORTED
        'config drift during read' | { it } | [configsStable:[orders:UNAVAILABLE]] | CONFIG_CHANGED
    }

    @Unroll
    void 'permission denial performs no metadata or writes manage=#manage allow=#allow noDelete=#noDelete'() {
        given:
        def f = fixture([[0,1,2]],2)
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ throw new AssertionError('No metadata allowed') })

        when:
        new ApplyManager(settings(manage,allow,noDelete),kafka).applyTopics(combined(f.plan))

        then:
        thrown(ValidationException)
        kafka.reads == 0
        kafka.writes == 0

        where:
        manage | allow | noDelete
        false | false | false
        false | true | false
        true | false | false
        true | true | true
    }

    void 'READY preflight returns execution not implemented and performs zero writes'() {
        given:
        def f = fixture()
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ observed(f.base) })

        when:
        new ApplyManager(settings(),kafka).applyTopics(combined(f.plan))

        then:
        def error = thrown(ReplicationPreflightException)
        error.outcome == ReplicationPreflightException.Outcome.EXECUTION_NOT_IMPLEMENTED
        error.message.contains('RF execution not implemented')
        error.results.orders.status() == ReplicationSafetyResult.Status.READY
        kafka.reads == 1
        kafka.writes == 0
    }

    void 'one batch snapshot is shared across RF topics and one failure blocks the batch'() {
        given:
        def f = fixture()
        def duplicate = new TopicPlan.Builder().setName('other-topic').setAction(UPDATE).setReplicationPlan(f.plan).build()
        def desired = new DesiredPlan.Builder().mergeFrom(combined(f.plan)).addTopicPlans(duplicate).build()
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ observed(f.base) })

        when:
        new ApplyManager(settings(),kafka).applyTopics(desired)

        then:
        def error = thrown(ReplicationPreflightException)
        error.outcome == ReplicationPreflightException.Outcome.BLOCKED
        error.results['other-topic'].reasons()*.code().contains(TOPIC_NOT_FOUND)
        kafka.names == ['orders','other-topic'] as Set
        kafka.reads == 1
        kafka.writes == 0
    }

    void 'permission failure never marks unexamined sibling RF operations READY'() {
        given:
        def increase = fixture()
        def decrease = fixture([[0,1,2]],2)
        def desired = new DesiredPlan.Builder().mergeFrom(combined(increase.plan)).addTopicPlans(
                new TopicPlan.Builder().setName('decreasing-topic').setAction(UPDATE).setReplicationPlan(decrease.plan).build()).build()
        def kafka = new NoWritePreflightKafka()

        when:
        new ApplyManager(settings(true,false),kafka).applyTopics(desired)

        then:
        def error = thrown(ReplicationPreflightException)
        error.results.orders.reasons()*.code() == [PREFLIGHT_NOT_PERFORMED]
        error.results.values().every { it.status() == ReplicationSafetyResult.Status.BLOCKED }
        error.results.values().every { result -> result.operations().every { it.status() == ReplicationSafetyResult.Status.BLOCKED } }
        kafka.reads == 0
        kafka.writes == 0
    }

    void 'metadata failures cannot leak raw broker response or allow earlier config writes'() {
        given:
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ throw new KafkaExecutionException('private-jaas','private-password') })

        when:
        new ApplyManager(settings(),kafka).applyTopics(combined(fixture().plan))

        then:
        def error = thrown(ReplicationPreflightException)
        error.results.orders.reasons()*.code() == [METADATA_UNAVAILABLE]
        !error.message.contains('private-')
        kafka.writes == 0
    }

    void 'unexpected provider failure also becomes a safe typed rejection'() {
        given:
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ throw new IllegalStateException('private-provider-secret') })

        when:
        new ApplyManager(settings(),kafka).applyTopics(combined(fixture().plan))

        then:
        def error = thrown(ReplicationPreflightException)
        error.results.orders.reasons()*.code() == [METADATA_UNAVAILABLE]
        !error.message.contains('private-')
        kafka.writes == 0
    }

    void 'changed ordered replicas and lost decrease leader block writes in mixed RF topic'() {
        given:
        def f = fixture([[0],[1,2,3]],2)
        def current = topic(f.base,'orders',[partitions:[f.base.topics().orders.partitions().first(),
                new Partition(1,[3,2,1],OptionalInt.of(3),[1,2,3] as Set)]])
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ observed(current) })

        when:
        new ApplyManager(settings(),kafka).applyTopics(combined(f.plan))

        then:
        def error = thrown(ReplicationPreflightException)
        error.results.orders.reasons()*.code().contains(PREVIOUS_REPLICAS_CHANGED)
        error.results.orders.reasons()*.code().contains(LEADER_NOT_RETAINED)
        kafka.writes == 0
    }

    @Unroll
    void 'critical config #key cannot accompany RF even if JSON requests it'() {
        given:
        def f = fixture()
        def desired = new DesiredPlan.Builder().addTopicPlans(new TopicPlan.Builder().setName('orders').setAction(UPDATE)
                .setReplicationPlan(f.plan).addTopicConfigPlans(config(key,UPDATE,'private-desired','private-old')).build()).build()
        def kafka = new NoWritePreflightKafka(snapshotFactory:{ observed(f.base) })

        when:
        new ApplyManager(settings(),kafka).applyTopics(desired)

        then:
        def error = thrown(ReplicationPreflightException)
        error.results.orders.reasons()*.code().contains(CONFIG_CONFLICT)
        !error.message.contains('private-')
        kafka.writes == 0

        where:
        key << ['min.insync.replicas','unclean.leader.election.enable','leader.replication.throttled.replicas','follower.replication.throttled.replicas']
    }

    void 'partition conflict is rejected structurally before metadata or writes'() {
        given:
        def topic = new TopicPlan.Builder().setName('orders').setAction(UPDATE).setReplicationPlan(fixture().plan)
                .setPartitionPlan(new TopicPartitionPlan.Builder().setAction(UPDATE).setPreviousCount(2).setTargetCount(3).build()).build()
        def kafka = new NoWritePreflightKafka()

        when:
        new ApplyManager(settings(),kafka).applyTopics(new DesiredPlan.Builder().addTopicPlans(topic).build())

        then:
        thrown(IllegalArgumentException)
        kafka.reads == 0
        kafka.writes == 0
    }

    void 'RF preflight also blocks direct ACL writes when topics were skipped'() {
        given:
        def acl = new AclPlan.Builder().setName('acl').setAction(ADD).setAclDetails(new AclDetails.Builder()
                .setName('orders').setType('TOPIC').setPattern('LITERAL').setPrincipal('User:test')
                .setHost('*').setOperation('READ').setPermission('ALLOW').build()).build()
        def desired = new DesiredPlan.Builder().mergeFrom(combined(fixture().plan)).addAclPlans(acl).build()
        def config = new ManagerConfig.Builder().mergeFrom(settings(false,false)).setSkipTopicsDisabled(true).build()
        def kafka = new NoWritePreflightKafka()

        when:
        new ApplyManager(config,kafka).applyAcls(desired)

        then:
        thrown(ReplicationPreflightException)
        kafka.reads == 0
        kafka.writes == 0
    }

    @Unroll
    void 'new apply flags parse without authorizing execution #args'() {
        expect:
        new CommandLine(new MainCommand()).parseArgs('apply',*args).subcommand().hasMatchedOption('--manage-replication')

        where:
        args << [['--manage-replication'],['--manage-replication','--allow-replication-decrease']]
    }

    void 'allow decrease without manage is a local CLI error before configuration or Kafka'() {
        expect:
        new CommandLine(new MainCommand()).execute('apply','--allow-replication-decrease') == 2
    }
}

/** A real Kafka client cannot be created, and every mutator fails if the preflight boundary is crossed. */
class NoWritePreflightKafka extends KafkaService {
    Closure snapshotFactory
    int reads
    int writes
    Set<String> names
    NoWritePreflightKafka() { super(new KafkaGitopsConfig.Builder().build()) }
    @Override protected Admin buildAdminClient() { throw new AssertionError('Unexpected Admin creation') }
    @Override ReplicationClusterSnapshot readReplicationPreflightSnapshot(Set<String> requested) {
        reads++; names = requested; snapshotFactory.call()
    }
    @Override Map describeTopics(List names) { throw new AssertionError('Legacy partition flow reached') }
    @Override void createTopic(String name, TopicDetails details) { writes++; throw new AssertionError('createTopic') }
    @Override void deleteTopic(String name) { writes++; throw new AssertionError('deleteTopic') }
    @Override void updateTopicConfig(Map configs) { writes++; throw new AssertionError('updateTopicConfig') }
    @Override boolean increaseTopicPartitions(String name, int previous, int target) { writes++; throw new AssertionError('increaseTopicPartitions') }
    @Override void createAcl(org.apache.kafka.common.acl.AclBinding acl) { writes++; throw new AssertionError('createAcl') }
    @Override boolean deleteAcl(org.apache.kafka.common.acl.AclBinding acl) { writes++; throw new AssertionError('deleteAcl') }
}
