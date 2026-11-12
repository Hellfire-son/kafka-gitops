package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.PartitionReassignment
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.mapper
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.permissions
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*

/** Controlled interleavings against proxy Admin only; every write assertion concerns in-memory calls. */
class ReplicationRegressionAuditSpec extends Specification {
    private static List<TopicPlan> topics(ReplicationApplicationTestHarness h) {
        h.request.operations().collect { new TopicPlan.Builder().setName(it.topicName()).setAction(UPDATE).setReplicationPlan(it.plan()).build() }
    }

    @Unroll
    void 'audit exact execution #previous -> #target preserves unchanged partitions and closes once'() {
        given:
        def h = new ReplicationApplicationTestHarness([orders:previous],[orders:target])

        when:
        def result = h.application().apply(topics(h),permissions())

        then:
        result.state() == State.COMPLETED
        h.opens == 1
        h.fake.closes == 1
        h.fake.writes.collectMany { it.entrySet() }.every { it.value == h.targets[it.key] }
        h.targets.each { key,assignment ->
            assert result.partitions()[key].submission() == (h.previous[key] == assignment ? Submission.OBSERVED : Submission.SENT)
        }

        where:
        previous | target
        [[0],[1]] | 2
        [[0,1],[1,0]] | 3
        [[0,1,2],[1,2,0]] | 2
        [[0,1,2],[1,2,0]] | 1
        [[0],[1,2,3]] | 2
        [[0],[1,2]] | 2
    }

    @Unroll
    void 'audit preflight #description never calls any mutating Admin method'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.snapshotMutation = mutation
        h.evidence = evidence

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationPreflightException)
        error.message.contains(reason)
        h.executionCalls == 0
        h.fake.writes.isEmpty()
        h.fake.closes == 1

        where:
        description | mutation | evidence | reason
        'unknown cluster ID' | { changed(it,[clusterId:null]) } | [:] | 'CLUSTER_ID_CHANGED'
        'unknown topic UUID' | { topic(it,'orders',[topicId:null]) } | [:] | 'TOPIC_ID_CHANGED'
        'partition count drift' | { topic(it,'orders',[partitions:it.topics().orders.partitions().take(1)]) } | [:] | 'PARTITION_COUNT_CHANGED'
        'broker unavailable' | { broker(it,3,[reachable:UNAVAILABLE]) } | [:] | 'BROKER_OFFLINE'
        'target below effective min ISR' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(4)]) } | [:] | 'TARGET_BELOW_MIN_ISR'
        'unknown disk bytes' | { broker(it,3,[minimumUsableBytes:OptionalLong.empty()]) } | [:] | 'DISK_CAPACITY_UNKNOWN'
        'fencing unknown' | { broker(it,3,[unfenced:UNKNOWN]) } | [:] | 'BROKER_FENCING_UNKNOWN'
        'admission unknown' | { it } | [admissions:[:]] | 'PLACEMENT_UNKNOWN'
        'ELR unknown' | { it } | [elrMode:ReplicationSafetyEvidence.ElrMode.UNKNOWN] | 'ELR_MODE_UNKNOWN'
    }

    void 'historical outer READY cannot bypass the fresh executor safety check'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.snapshotMutation = { s -> h.snapshotReads > 1 ? broker(s,3,[unfenced:UNKNOWN]) : s }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationExecutionException)
        error.result.partitions().values().every { it.error() == ErrorCategory.PREFLIGHT_BLOCKED }
        h.executionCalls == 1
        h.fake.writes.isEmpty()
        h.fake.closes == 1
    }

    @Unroll
    void 'race #description after fresh safety but before submit is detected without writes'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        Throwable hookFailure = null
        h.fake.onDescribe = { int call ->
            if (call == 2) {
                try { mutation.call(h) }
                catch (Throwable error) { hookFailure = error; throw error }
            }
        }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationExecutionException)
        hookFailure == null
        error.result.state() != State.COMPLETED
        error.result.partitions().values().any { it.error() == ErrorCategory.STALE_PLAN }
        h.fake.writes.isEmpty()
        h.fake.closes == 1

        where:
        description | mutation
        'replica assignment' | { probe -> probe.fake.setPartition(new TopicPartition('orders',0),[3,2],[3,2]) }
        'ISR loss' | { probe -> probe.fake.setPartition(new TopicPartition('orders',0),[0,1],[0]) }
        'topic identity' | { probe -> probe.fake.current = topic(probe.fake.current,'orders',[topicId:'ERITFBUWFxgZGhscHR4fIA']) }
    }

    @Unroll
    void 'race #description after submission cannot be reported as completed or rolled back'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.batchSize = 2
        h.fake.afterWrite = { request -> mutation.call(h) }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationExecutionException)
        error.result.state() != State.COMPLETED
        error.result.partitions().values().any { it.error() == expected }
        error.message.contains('Requests may have been accepted')
        h.fake.writes.size() == 1
        h.fake.writes.first() == h.targets
        h.fake.closes == 1

        where:
        description | mutation | expected
        'topic recreation' | { probe -> probe.fake.current = topic(probe.fake.current,'orders',[topicId:'ERITFBUWFxgZGhscHR4fIA']) } | ErrorCategory.STALE_PLAN
        'foreign reassignment' | { probe -> probe.fake.active[new TopicPartition('orders',0)] = new PartitionReassignment([0,1,3],[3],[]) } | ErrorCategory.CONFLICT
    }

    @Unroll
    void 'independent final verification rejects #description after executor completed'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.executeOverride = { engine,request ->
            def result = engine.execute(request)
            assert result.state() == State.COMPLETED
            mutation.call(h)
            result
        }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationExecutionException)
        error.phase == ReplicationExecutionException.Phase.VERIFICATION
        error.result.state() == State.UNKNOWN
        !error.message.contains('private-')
        h.fake.closes == 1
        h.fake.writes.size() == 2

        where:
        description | mutation
        'ISR loss' | { probe -> probe.fake.setPartition(new TopicPartition('orders',0),probe.targets[new TopicPartition('orders',0)],[0]) }
        'assignment drift' | { probe -> probe.fake.setPartition(new TopicPartition('orders',0),[0,1],[0,1]) }
        'cluster change' | { probe -> probe.fake.current = changed(probe.fake.current,[clusterId:'private-replaced-cluster']) }
        'new active reassignment' | { probe -> probe.fake.active[new TopicPartition('orders',0)] = new PartitionReassignment([0,1,3],[3],[]) }
        'operation deadline' | { probe -> probe.clock.now += 100000000L }
    }

    void 'connection loss while observing accepted work returns UNKNOWN and closes once'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.batchSize = 2
        h.fake.autoComplete = false
        h.fake.onDescribe = { int call -> if (call == 3) throw new org.apache.kafka.common.errors.NetworkException('private-network-response') }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationExecutionException)
        error.result.state() == State.UNKNOWN
        error.result.partitions().values().every { it.error() == ErrorCategory.BROKER_UNAVAILABLE && it.submission() == Submission.SENT }
        !error.message.contains('private-')
        h.fake.writes.size() == 1
        h.fake.closes == 1
    }

    void 'rerun after partial failure observes the completed partition and sends only the remaining one'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.batchSize = 2
        def failed = new TopicPartition('orders',1)
        h.fake.errors[failed] = new org.apache.kafka.common.errors.ClusterAuthorizationException('private-denied')
        def plan = topics(h)

        when:
        h.application().apply(plan,permissions())

        then:
        def first = thrown(ReplicationExecutionException)
        first.result.partiallyCompleted()

        when:
        h.fake.errors.clear()
        def result = h.application().apply(plan,permissions())

        then:
        result.state() == State.COMPLETED
        result.partitions()[new TopicPartition('orders',0)].submission() == Submission.OBSERVED
        result.partitions()[failed].submission() == Submission.SENT
        h.fake.writes.size() == 2
        h.fake.writes.last().keySet() == [failed] as Set
        h.opens == 2
        h.fake.closes == 2
    }

    void 'preflight interruption restores status without starting execution'() {
        given:
        def h = new ReplicationApplicationTestHarness()
        h.snapshotMutation = { throw new InterruptedException('private-preflight-response') }

        when:
        h.application().apply(topics(h),permissions())

        then:
        def error = thrown(ReplicationPreflightException)
        Thread.currentThread().isInterrupted()
        !error.message.contains('private-')
        h.executionCalls == 0
        h.fake.writes.isEmpty()
        h.fake.closes == 1

        cleanup:
        Thread.interrupted()
    }

    void 'planning remains read only with one owned Admin and real strict metadata reader'() {
        given:
        def fake = new ReadOnlyAdmin()
        def service = new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }

        when:
        def current = service.readReplicationSnapshot(['orders'] as Set)
        def result = new ReplicaAssignmentPlanner().plan(current,[orders:2],RACK_AWARE,false,false)

        then:
        result.orders.assignments.size() == 2
        fake.calls.count('close') == 1
        !fake.calls.any { it.startsWith('alter') || it.startsWith('create') || it.startsWith('delete') || it.startsWith('incremental') }
        current.brokers().values().every { it.unfenced() == VERIFIED }
    }

    void 'planner output is invariant under map set and partition observation order'() {
        given:
        def initial = snapshot([zeta:[[0],[1,2,3]],alpha:[[2,1],[3]]])
        def planner = new ReplicaAssignmentPlanner()
        def expected = mapper().writeValueAsString(planner.plan(initial,[zeta:2,alpha:2],RACK_AWARE,true,false))

        expect:
        (0..<24).every { int seed ->
            def random = new Random(seed)
            def entries = { Map map ->
                def list = new ArrayList(map.entrySet()); Collections.shuffle(list,random)
                list.collectEntries { [(it.key):it.value] }
            }
            def reorderedTopics = initial.topics().collectEntries { name,t ->
                def partitions = new ArrayList(t.partitions()); Collections.shuffle(partitions,random)
                [(name):new com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Topic(t.topicId(),partitions,t.minInSyncReplicas())]
            }
            def reordered = changed(initial,[brokers:entries(initial.brokers()),topics:entries(reorderedTopics),replicaCounts:entries(initial.replicaCounts())])
            mapper().writeValueAsString(planner.plan(reordered,entries([zeta:2,alpha:2]),RACK_AWARE,true,false)) == expected
        }
    }
}
