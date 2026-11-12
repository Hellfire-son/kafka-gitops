package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.*
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.TimeUnit

import static com.devshawn.kafka.gitops.PlanTestSupport.mapper
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*

class ReassignmentExecutorSpec extends Specification {
    private static ExecutorHarness harness(Map assignments = [orders:[[0],[1]]], Map targets = [orders:2], int batchSize = 100) {
        new ExecutorHarness(assignments,targets,batchSize)
    }

    @Unroll
    void 'executes exact #previous -> #target assignments and verifies completion'() {
        given:
        def h = harness([orders:previous],[orders:target])

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        result.partitions().values().every { it.state() == State.COMPLETED && it.submission() == Submission.SENT }
        h.fake.writes.size() == 1
        h.fake.writes.first() == h.targets
        h.snapshotReads == 1
        !h.fake.calls.contains('close')
        h.fake.calls.count('describeTopics') >= 3

        where:
        previous | target
        [[0],[1]] | 2
        [[0,1],[1,0]] | 3
        [[0,1,2],[1,2,0]] | 2
    }

    void 'multiple topics and partitions use stable bounded sequential batches'() {
        given:
        def h = harness([zeta:[[0],[1]],alpha:[[2],[3]]],[zeta:2,alpha:2],1)

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        h.fake.writes*.keySet()*.toList() == [[tp('alpha',0)],[tp('alpha',1)],[tp('zeta',0)],[tp('zeta',1)]]
        h.snapshotReads == 4
        h.fake.writes.every { it.size() == 1 }
    }

    void 'acceptance alone is not completion and no active entry alone is not completion'() {
        given:
        def h = harness()
        h.fake.autoComplete = false
        h.fake.publishActive = false

        when:
        def result = h.run(30)

        then:
        result.state() == State.UNKNOWN
        result.partitions().values().every { it.state() == State.UNKNOWN && it.lastKnownState() == State.ACCEPTED && it.error() == ErrorCategory.DEADLINE_EXCEEDED }
        h.fake.writes.size() == 1
        h.clock.pauses > 0
    }

    void 'active movement then incomplete target ISR then full ISR is observed without resubmission'() {
        given:
        def h = harness()
        h.fake.autoComplete = false
        h.clock.onPause = { int step ->
            if (step == 1) {
                h.targets.each { key,target -> h.fake.setPartition(key,target,h.previous[key]); h.fake.active.clear() }
            }
            if (step == 2) h.targets.each { key,target -> h.fake.setPartition(key,target,target) }
        }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        h.clock.pauses == 2
        h.fake.writes.size() == 1
    }

    void 'partial acceptance stops later batches and observes accepted siblings'() {
        given:
        def h = harness([orders:[[0],[1],[2]]],[orders:2],2)
        h.fake.autoComplete = false
        h.fake.errors[tp('orders',1)] = new ClusterAuthorizationException('private-denied')
        h.clock.onPause = { int step -> h.fake.finish(tp('orders',0),h.targets[tp('orders',0)]) }

        when:
        def result = h.run()

        then:
        result.state() == State.FAILED
        result.partiallyCompleted()
        result.partitions()[tp('orders',0)].state() == State.COMPLETED
        result.partitions()[tp('orders',1)].error() == ErrorCategory.AUTHORIZATION
        result.partitions()[tp('orders',2)].error() == ErrorCategory.NOT_SUBMITTED
        result.partitions()[tp('orders',2)].submission() == Submission.NOT_SENT
        h.fake.writes.size() == 1
    }

    @Unroll
    void 'individual future failure #failure is typed #expected and never retried'() {
        given:
        def h = harness([orders:[[0]]],[orders:2])
        h.fake.errors[tp('orders',0)] = failure

        when:
        def result = h.run(20)
        def outcome = result.partitions()[tp('orders',0)]

        then:
        outcome.error() == expected
        outcome.state() == state
        h.fake.writes.size() == 1
        !result.toString().contains('private-')

        where:
        failure | expected | state
        new ClusterAuthorizationException('private-auth') | ErrorCategory.AUTHORIZATION | State.FAILED
        new InvalidReplicaAssignmentException('private-assignment') | ErrorCategory.VALIDATION | State.FAILED
        new UnsupportedVersionException('private-api') | ErrorCategory.UNSUPPORTED | State.FAILED
        new UnknownTopicOrPartitionException('private-topic') | ErrorCategory.STALE_PLAN | State.FAILED
        new org.apache.kafka.common.errors.TimeoutException('private-rpc') | ErrorCategory.DEADLINE_EXCEEDED | State.UNKNOWN
        new NetworkException('private-network') | ErrorCategory.DEADLINE_EXCEEDED | State.UNKNOWN
    }

    void 'RPC timeout consumes one shared RPC budget while preserving completed sibling futures'() {
        given:
        def h = harness()
        h.fake.futureOverrides[tp('orders',0)] = new DeadlineFuture(h.clock)

        when:
        def result = h.run(20)

        then:
        result.partitions()[tp('orders',0)].state() == State.UNKNOWN
        result.partitions()[tp('orders',0)].error() == ErrorCategory.DEADLINE_EXCEEDED
        result.partitions()[tp('orders',1)].state() == State.COMPLETED
        h.fake.writes.size() == 1
        h.fake.rpcTimeouts.every { it > 0 && it <= 10 }
    }

    void 'timeout with target and incomplete ISR preserves RUNNING as last known state'() {
        given:
        def h = harness([orders:[[0]]],[orders:2])
        h.fake.autoComplete = false
        h.fake.publishActive = false
        h.fake.afterWrite = { Map request -> request.each { key,target -> h.fake.setPartition(key,target,h.previous[key]) } }

        when:
        def result = h.run(25)

        then:
        result.state() == State.UNKNOWN
        result.partitions().values().first().lastKnownState() == State.RUNNING
        result.partitions().values().first().error() == ErrorCategory.DEADLINE_EXCEEDED
        h.fake.writes.size() == 1
    }

    @Unroll
    void 'BLOCKED fresh policy #caseName sends zero writes'() {
        given:
        def h = harness()
        h.snapshotMutation = mutation
        h.evidence = evidence

        when:
        def result = h.run()

        then:
        result.state() == State.FAILED
        result.partitions().values().every { it.error() == ErrorCategory.PREFLIGHT_BLOCKED }
        h.fake.writes.isEmpty()

        where:
        caseName | mutation | evidence
        'fencing unknown' | { broker(it,2,[unfenced:com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN]) } | [:]
        'offline broker' | { broker(it,2,[reachable:com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNAVAILABLE]) } | [:]
        'no placement proof' | { it } | [admissions:[:]]
        'no timestamp' | { it } | [completedAtNanos:OptionalLong.empty()]
        'old timestamp' | { it } | [completedAtNanos:OptionalLong.of(1L)]
        'min ISR unknown' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.empty()]) } | [:]
        'incomplete original ISR' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.of(0),[] as Set),it.topics().orders.partitions().last()]]) } | [:]
        'ELR unknown' | { it } | [elrMode:ReplicationSafetyEvidence.ElrMode.UNKNOWN]
        'ELR enabled' | { it } | [elrMode:ReplicationSafetyEvidence.ElrMode.ENABLED]
    }

    void 'already completed transfer is a read-only no-op with explicit observed submission'() {
        given:
        def h = harness()
        h.targets.each { key,target -> h.fake.finish(key,target) }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        result.partitions().values().every { it.submission() == Submission.OBSERVED }
        h.fake.writes.isEmpty()
        h.snapshotReads == 0
    }

    void 'existing compatible foreign reassignment is observed but never adopted or resent'() {
        given:
        def h = harness()
        h.targets.each { key,target -> h.fake.begin(key,h.previous[key],target) }
        h.clock.onPause = { int step -> h.targets.each { key,target -> h.fake.finish(key,target) } }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        result.partitions().values().every { it.submission() == Submission.OBSERVED }
        h.fake.writes.isEmpty()
    }

    void 'compatible decrease can remove a middle replica without falsifying original order'() {
        given:
        def h = harness([orders:[[0,1,2]]],[orders:2])
        def key = tp('orders',0)
        // Select a valid ordered-subset target which drops a middle replica.
        def original = h.request.operations().first()
        def replacement = new com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan.Builder().mergeFrom(original.plan())
                .clearAssignments().addAssignments(com.devshawn.kafka.gitops.ReplicationTestSupport.assignment(0,[0,1,2],[0,2])).build()
        h.replaceOperation('orders',replacement)
        h.fake.begin(key,[0,1,2],[0,2])
        h.clock.onPause = { int step -> h.fake.finish(key,[0,2]) }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        h.fake.writes.isEmpty()
    }

    void 'restart waits for incomplete target ISR then sends only still-original partitions'() {
        given:
        def h = harness()
        def first = tp('orders',0)
        h.fake.setPartition(first,h.targets[first],h.previous[first])
        h.clock.onPause = { int step -> h.fake.finish(first,h.targets[first]) }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        h.fake.writes*.keySet() == [[tp('orders',1)] as Set]
        result.partitions()[first].submission() == Submission.OBSERVED
        result.partitions()[tp('orders',1)].submission() == Submission.SENT
    }

    void 'conflicting reassignment prevents writes for all pending partitions'() {
        given:
        def h = harness()
        h.fake.active[tp('orders',0)] = new PartitionReassignment([0,3],[3],[])

        when:
        def result = h.run()

        then:
        result.state() == State.FAILED
        result.partitions()[tp('orders',0)].error() == ErrorCategory.CONFLICT
        h.fake.writes.isEmpty()
    }

    @Unroll
    void 'identity or assignment drift #caseName cannot write or report completion'() {
        given:
        def h = harness()
        mutation(h)

        when:
        def result = h.run()

        then:
        result.state() != State.COMPLETED
        result.partitions().values().any { it.error() == ErrorCategory.STALE_PLAN }
        h.fake.writes.isEmpty()

        where:
        caseName | mutation
        'cluster ID' | { it.fake.current = changed(it.fake.current,[clusterId:'different-cluster']) }
        'topic UUID' | { it.fake.current = topic(it.fake.current,'orders',[topicId:'ERITFBUWFxgZGhscHR4fIA']) }
        'partition count' | { it.fake.current = topic(it.fake.current,'orders',[partitions:it.fake.current.topics().orders.partitions().take(1)]) }
        'unplanned replicas' | { it.fake.setPartition(tp('orders',0),[3],[3]) }
    }

    void 'assignment changes after full safety validation prevent submission'() {
        given:
        def h = harness()
        h.fake.onDescribe = { int call -> if (call == 2) h.fake.setPartition(tp('orders',0),[3],[3]) }

        when:
        def result = h.run()

        then:
        result.state() == State.FAILED
        h.fake.writes.isEmpty()
    }

    void 'leader changes after safety validation prevent a stale decrease write'() {
        given:
        def h = harness([orders:[[0,1,2]]],[orders:1])
        h.fake.onDescribe = { int call -> if (call == 2) h.fake.setPartition(tp('orders',0),[0,1,2],[0,1,2],2) }

        expect:
        h.run().state() == State.FAILED
        h.fake.writes.isEmpty()
    }

    void 'RPC metadata timeout is unknown and does not send any writes'() {
        given:
        def h = harness()
        h.fake.readFailure = new org.apache.kafka.common.errors.TimeoutException('private-rpc')

        when:
        def result = h.run()

        then:
        result.state() == State.UNKNOWN
        result.partitions().values().every { it.error() == ErrorCategory.RPC_TIMEOUT }
        h.fake.writes.isEmpty()
    }

    void 'interrupt while awaiting movement restores flag and leaves submitted results unknown'() {
        given:
        def h = harness()
        h.fake.autoComplete = false
        h.clock.interruptPause = true

        when:
        def result = h.run()

        then:
        Thread.currentThread().isInterrupted()
        result.state() == State.UNKNOWN
        result.partitions().values().every { it.error() == ErrorCategory.INTERRUPTED }
        h.fake.writes.size() == 1

        cleanup:
        Thread.interrupted()
    }

    void 'raw broker exception messages cannot appear in diagnostics or application output'() {
        given:
        def h = harness()
        h.fake.submitFailure = new IllegalStateException('private-jaas password=private-token')
        def stream = new ByteArrayOutputStream()
        def out = System.out
        def err = System.err
        ExecutionResult result

        when:
        try {
            System.setOut(new PrintStream(stream,true,'UTF-8')); System.setErr(new PrintStream(stream,true,'UTF-8'))
            result = h.run(20)
        } finally { System.setOut(out); System.setErr(err) }

        then:
        !stream.toString('UTF-8').contains('private-')
        !result.toString().contains('private-')
        result.state() == State.UNKNOWN
        h.fake.writes.size() == 1
    }

    void 'elapsed operation deadline is checked before creating requests'() {
        given:
        def h = harness()
        def request = new Request(h.request.operations(),h.request.permissions(),h.clock.now - 100L,h.clock.now - 1L)

        when:
        def result = h.executor.execute(request)

        then:
        result.state() == State.UNKNOWN
        h.fake.writes.isEmpty()
        h.fake.calls.isEmpty()
    }

    void 'an already interrupted caller cannot start reads or writes'() {
        given:
        def h = harness()
        Thread.currentThread().interrupt()

        when:
        def result = h.run()

        then:
        Thread.currentThread().isInterrupted()
        result.partitions().values().every { it.error() == ErrorCategory.INTERRUPTED }
        h.fake.calls.isEmpty()
        h.fake.writes.isEmpty()

        cleanup:
        Thread.interrupted()
    }

    void 'fresh safety is mandatory again after a completed batch'() {
        given:
        def h = harness([orders:[[0],[1]]],[orders:2],1)
        h.snapshotMutation = { current -> h.snapshotReads > 1 ? broker(current,3,
                [unfenced:com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN]) : current }

        when:
        def result = h.run()

        then:
        result.partitions()[tp('orders',0)].state() == State.COMPLETED
        result.partitions()[tp('orders',1)].error() == ErrorCategory.PREFLIGHT_BLOCKED
        result.state() == State.FAILED
        h.fake.writes.size() == 1
    }

    void 'preflight that consumes the overall deadline cannot send a write afterward'() {
        given:
        def h = harness()
        h.snapshotMutation = { current -> h.clock.now += TimeUnit.MILLISECONDS.toNanos(100); current }

        when:
        def result = h.run(20)

        then:
        result.state() == State.UNKNOWN
        result.partitions().values().every { it.error() == ErrorCategory.DEADLINE_EXCEEDED }
        h.fake.writes.isEmpty()
    }

    void 'interrupt in a partition future preserves earlier acceptance and stops new packets'() {
        given:
        def h = harness([orders:[[0],[1],[2]]],[orders:2],2)
        h.fake.futureOverrides[tp('orders',1)] = new InterruptFuture()
        h.fake.autoComplete = false

        when:
        def result = h.run()

        then:
        Thread.currentThread().isInterrupted()
        result.partitions()[tp('orders',0)].lastKnownState() == State.ACCEPTED
        result.partitions()[tp('orders',0)].state() == State.UNKNOWN
        result.partitions()[tp('orders',1)].error() == ErrorCategory.INTERRUPTED
        result.partitions()[tp('orders',2)].submission() == Submission.NOT_SENT
        h.fake.writes.size() == 1

        cleanup:
        Thread.interrupted()
    }

    void 'cluster identity change after submission leaves result unknown and never sends another batch'() {
        given:
        def h = harness([orders:[[0],[1]]],[orders:2],1)
        h.fake.autoComplete = false
        h.fake.onDescribe = { int call -> if (call == 3) h.fake.current = changed(h.fake.current,[clusterId:'changed-cluster']) }

        when:
        def result = h.run()

        then:
        result.state() != State.COMPLETED
        result.partitions()[tp('orders',0)].submission() == Submission.SENT
        h.fake.writes.size() == 1
    }

    void 'a transport timeout can be resolved only by observed target completion'() {
        given:
        def h = harness([orders:[[0]]],[orders:2])
        h.fake.errors[tp('orders',0)] = new org.apache.kafka.common.errors.TimeoutException('private-ack')
        h.clock.onPause = { int count -> h.fake.finish(tp('orders',0),h.targets[tp('orders',0)]) }

        when:
        def result = h.run()

        then:
        result.state() == State.COMPLETED
        result.partitions().values().first().submission() == Submission.SENT
        h.fake.writes.size() == 1
    }

    void 'permissions are required even if a caller holds a historical READY result'() {
        given:
        def h = harness([orders:[[0,1,2]]],[orders:2])

        when:
        new Request(h.request.operations(),permissions(true,false,false),h.clock.now,h.clock.now + 100L)

        then:
        thrown(IllegalArgumentException)
        h.fake.writes.isEmpty()
    }

    void 'default reader keeps real 3_9 UNKNOWN safety blocked despite a supplied historical READY'() {
        given:
        def h = harness()
        def admin = new PreflightReadOnlyAdmin()
        admin.brokerEntries[1] = [] // No proof of cordoning applicability/clearance.
        def executor = new AdminReassignmentExecutor(admin.admin)
        long now = System.nanoTime()

        when:
        def result = executor.execute(new Request(h.request.operations(),permissions(),now,now + TimeUnit.SECONDS.toNanos(1)))

        then:
        result.state() == State.FAILED
        result.partitions().values().every { it.error() == ErrorCategory.PREFLIGHT_BLOCKED }
        !admin.calls.contains('alterPartitionReassignments')
        !admin.calls.contains('close')
    }

    void 'rerun after UNKNOWN rereads state and does not resend an already completed request'() {
        given:
        def h = harness()
        h.fake.autoComplete = false

        when:
        def first = h.run(20)
        h.targets.each { key,target -> h.fake.finish(key,target) }
        def second = h.run(20)

        then:
        first.state() == State.UNKNOWN
        second.state() == State.COMPLETED
        second.partitions().values().every { it.submission() == Submission.OBSERVED }
        h.fake.writes.size() == 1
    }

    @Unroll
    void 'invalid bounded executor settings #batch #rpc #poll are rejected without Admin use'() {
        given:
        def h = harness()

        when:
        new AdminReassignmentExecutor.Settings(batch,Duration.ofMillis(rpc),Duration.ofMillis(poll))

        then:
        thrown(IllegalArgumentException)
        h.fake.calls.isEmpty()

        where:
        batch | rpc | poll
        0 | 10 | 5
        1001 | 10 | 5
        1 | 0 | 5
        1 | 10 | 0
    }

    @Unroll
    void 'completion waits for a valid target leader #leader'() {
        given:
        def h = harness([orders:[[0]]],[orders:2])
        def key = tp('orders',0)
        h.fake.setPartition(key,h.targets[key],h.targets[key],leader)

        when:
        def result = h.run(20)

        then:
        result.state() == State.UNKNOWN
        result.partitions()[key].lastKnownState() == State.RUNNING
        h.fake.writes.isEmpty()

        where:
        leader << [-1,3]
    }

    private static TopicPartition tp(String topic,int partition) { new TopicPartition(topic,partition) }
}

class ExecutorClock implements AdminReassignmentExecutor.TimeSource {
    long now = 1000000000L
    int pauses
    boolean interruptPause
    Closure onPause
    @Override long nanoTime() { now }
    @Override void pause(long nanos) {
        if (interruptPause) throw new InterruptedException('private-interrupt')
        assert nanos > 0
        now += nanos
        pauses++
        onPause?.call(pauses)
    }
}

class DeadlineFuture extends KafkaFutureImpl<Void> {
    ExecutorClock clock
    DeadlineFuture(ExecutorClock clock) { this.clock = clock }
    @Override Void get(long timeout, TimeUnit unit) {
        clock.now += unit.toNanos(timeout)
        throw new java.util.concurrent.TimeoutException('private-future-timeout')
    }
}

class InterruptFuture extends KafkaFutureImpl<Void> {
    @Override Void get(long timeout, TimeUnit unit) { throw new InterruptedException('private-future-interrupt') }
}

class ExecutorHarness {
    ExecutorClock clock = new ExecutorClock()
    ExecutorAdmin fake
    ReassignmentExecutor.Request request
    AdminReassignmentExecutor executor
    Map<TopicPartition,List<Integer>> targets = [:]
    Map<TopicPartition,List<Integer>> previous = [:]
    int snapshotReads
    Closure snapshotMutation = { it }
    Map evidence = [:]

    ExecutorHarness(Map assignments,Map desired,int batchSize) {
        fake = new ExecutorAdmin(current:snapshot(assignments))
        def plans = new ReplicaAssignmentPlanner().plan(fake.current,desired,
                com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE,true,false)
        def operations = plans.collect { name,plan -> prepared(name,plan) }
        request = new ReassignmentExecutor.Request(operations,permissions(),clock.now,clock.now + 100000000L)
        refreshTargets()
        AdminReassignmentExecutor.SnapshotReader reader = { Admin supplied,Set names,int timeout ->
            assert supplied.is(fake.admin)
            assert timeout > 0 && timeout <= 10
            snapshotReads++
            observed(snapshotMutation(fake.current),[completedAtNanos:OptionalLong.of(clock.now)] + evidence)
        }
        executor = new AdminReassignmentExecutor(fake.admin,new AdminReassignmentExecutor.Settings(batchSize,Duration.ofMillis(10),Duration.ofMillis(5)),clock,reader)
    }

    private ReassignmentExecutor.PreparedOperation prepared(String name,plan) {
        def ready = new ReplicationSafetyPolicy().evaluate(name,plan,observed(fake.current),permissions(),constraints())
        assert ready.status() == ReplicationSafetyResult.Status.READY
        new ReassignmentExecutor.PreparedOperation(name,plan,ready)
    }
    void refreshTargets() {
        request.operations().each { operation -> operation.plan().assignments.each { a ->
            def key = new TopicPartition(operation.topicName(),a.partition)
            targets[key] = a.targetReplicas; previous[key] = a.previousReplicas
        } }
    }
    void replaceOperation(String name,plan) {
        request = new ReassignmentExecutor.Request([prepared(name,plan)],request.permissions(),request.startedNanos(),request.deadlineNanos())
        refreshTargets()
    }
    ReassignmentExecutor.ExecutionResult run(long durationMs = 100) {
        executor.execute(new ReassignmentExecutor.Request(request.operations(),request.permissions(),clock.now,clock.now + TimeUnit.MILLISECONDS.toNanos(durationMs)))
    }
}

/** A strict fake: exactly three read methods and alterPartitionReassignments. No network or client factory. */
class ExecutorAdmin implements InvocationHandler {
    ReplicationClusterSnapshot current
    List<String> calls = []
    List<Map<TopicPartition,List<Integer>>> writes = []
    Map<TopicPartition,PartitionReassignment> active = [:]
    Map<TopicPartition,Throwable> errors = [:]
    Map<TopicPartition,KafkaFuture<Void>> futureOverrides = [:]
    List<Integer> rpcTimeouts = []
    boolean autoComplete = true
    boolean publishActive = true
    Throwable readFailure
    Throwable submitFailure
    Closure onDescribe
    Closure afterWrite
    Admin admin = (Admin) Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],this)

    void setPartition(TopicPartition key,List<Integer> replicas,List<Integer> isr,Integer leader = null) {
        def old = current.topics()[key.topic()]
        def previousLeader = old.partitions().find { it.id() == key.partition() }.leader()
        int selectedLeader = leader != null ? leader : (previousLeader.present && replicas.contains(previousLeader.asInt) ? previousLeader.asInt : replicas.first())
        def partitions = old.partitions().collect { p -> p.id() == key.partition() ?
                new Partition(p.id(),replicas,OptionalInt.of(selectedLeader),new LinkedHashSet<>(isr)) : p }
        current = topic(current,key.topic(),[partitions:partitions])
    }
    void finish(TopicPartition key,List<Integer> target) { setPartition(key,target,target); active.remove(key) }
    void begin(TopicPartition key,List<Integer> previous,List<Integer> target) {
        def removing = previous.findAll { !target.contains(it) }
        def adding = target.findAll { !previous.contains(it) }
        def union = target + removing
        active[key] = new PartitionReassignment(union,adding,removing)
        setPartition(key,union,previous)
    }

    @Override Object invoke(Object proxy,Method method,Object[] args) {
        String name = method.name
        if (name == 'toString') return 'ExecutorAdmin'
        assert name in ['describeCluster','describeTopics','listPartitionReassignments','alterPartitionReassignments'] : name
        calls.add(name)
        rpcTimeouts.add(args.last().timeoutMs())
        if (name != 'alterPartitionReassignments' && readFailure) throw readFailure
        switch(name) {
            case 'describeCluster':
                return new DescribeClusterResult(null,null,KafkaFuture.completedFuture(current.clusterId()),null)
            case 'describeTopics':
                onDescribe?.call(calls.count(name))
                return new DescribeTopicsResult(null,args[0].collectEntries { n ->
                    [(n):KafkaFuture.completedFuture(description(n,current.topics()[n],current.brokers()))]
                })
            case 'listPartitionReassignments':
                return new ListPartitionReassignmentsResult(KafkaFuture.completedFuture(new LinkedHashMap<>(active)))
            case 'alterPartitionReassignments':
                Map request = args[0].collectEntries { key,value ->
                    assert value.present // Cancellation is never permitted.
                    [(key):new ArrayList<>(value.get().targetReplicas())]
                }
                writes.add(request)
                if (submitFailure) throw submitFailure
                Map futures = [:]
                request.each { key,target ->
                    if (futureOverrides.containsKey(key)) futures[key] = futureOverrides[key]
                    else if (errors.containsKey(key)) {
                        def future = new KafkaFutureImpl<Void>(); future.completeExceptionally(errors[key]); futures[key] = future
                    } else {
                        futures[key] = KafkaFuture.completedFuture(null)
                        if (!afterWrite) {
                            if (autoComplete) finish(key,target)
                            else if (publishActive) begin(key,current.topics()[key.topic()].partitions().find { it.id() == key.partition() }.replicas(),target)
                        }
                    }
                }
                afterWrite?.call(request)
                return new AlterPartitionReassignmentsResult(futures)
        }
        throw new AssertionError(name)
    }
}
