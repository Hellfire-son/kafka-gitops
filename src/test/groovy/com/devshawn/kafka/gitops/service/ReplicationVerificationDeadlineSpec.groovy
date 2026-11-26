// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import org.apache.kafka.clients.admin.AbstractOptions
import org.apache.kafka.clients.admin.DescribeFeaturesResult
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification

import java.lang.reflect.Method
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.LongSupplier

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.snapshot
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.ErrorCategory.*
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.NANOSECONDS

class ReplicationVerificationDeadlineSpec extends Specification {
    ExecutorClock clock = new ExecutorClock()
    DeadlineRecordingAdmin admin = new DeadlineRecordingAdmin(clock: clock,
            state: snapshot([orders: [[0, 1], [1, 0]]], [0: 'a', 1: 'b', 2: 'c']))

    void 'a #placement delay consumes the original #window ms window before the first RPC'() {
        given:
        def source = { ->
            if (admin.delayOnClockRead) {
                admin.delayOnClockRead = false
                clock.now += MILLISECONDS.toNanos(gap)
            }
            clock.nanoTime()
        } as LongSupplier
        long end = clock.now + MILLISECONDS.toNanos(window)
        def deadline = AdminReadDeadline.forVerification(source, end, MILLISECONDS.toNanos(window))
        if (placement == 'before reader entry') clock.now += MILLISECONDS.toNanos(gap)
        else admin.delayOnClockRead = true

        when:
        def observed = read(deadline)

        then:
        observed.topics().orders.partitions().size() == 2
        admin.requests.first().operation == 'describeFeatures'
        admin.requests.first().timeoutMs == window - gap
        deadline.deadlineNanos() == end
        admin.writes == 0

        where:
        placement             | window | gap
        'before reader entry' | 10000  | 8000
        'before reader entry' | 1000   | 750
        'inside reader'       | 10000  | 8000
        'inside reader'       | 1000   | 750
    }

    void 'every snapshot RPC floors its own remaining budget against the same absolute end'() {
        given:
        def deadline = budget(20)
        admin.dispatchNanos = 750_000L

        when:
        def observed = read(deadline)

        then:
        admin.requests*.operation == ['describeFeatures', 'describeCluster', 'describeTopics', 'listPartitionReassignments',
                'describeConfigs', 'describeLogDirs', 'describeCluster', 'describeTopics', 'listPartitionReassignments',
                'describeFeatures', 'describeConfigs']
        admin.requests*.timeoutMs == [20, 19, 18, 17, 17, 16, 15, 14, 14, 13, 12]
        admin.requests.every { it.timeoutMs == NANOSECONDS.toMillis(deadline.deadlineNanos() - it.sentAt) }
        observed.safety().completedAtNanos().getAsLong() == clock.now
        admin.writes == 0
    }

    void 'a remaining RPC budget of #remaining ns sends no zero or rounded-up timeout'() {
        given:
        def deadline = budget(10)
        clock.now = deadline.deadlineNanos() - remaining

        when:
        read(deadline)

        then:
        thrown(AdminReadDeadline.Exhausted)
        admin.requests.empty

        where:
        remaining << [999_999L, 0L, -1L]
    }

    void 'time spent dispatching an RPC is deducted before get and an unfinished future exhausts the deadline'() {
        given:
        def deadline = budget(10)
        admin.dispatchNanos = 7_250_000L
        def future = new DeadlineRecordingFuture({ long wait ->
            clock.now += wait
            throw new TimeoutException('local wait expired')
        })
        admin.featuresFuture = future

        when:
        read(deadline)

        then:
        def error = thrown(AdminReadDeadline.Exhausted)
        AdminReassignmentExecutor.category(error) == DEADLINE_EXCEEDED
        admin.requests*.timeoutMs == [10]
        future.waits == [2_750_000L]
        clock.now == deadline.deadlineNanos()
        admin.writes == 0
    }

    void 'expiry between the Admin call and the Future skips get entirely'() {
        given:
        def deadline = budget(10)
        admin.dispatchNanos = MILLISECONDS.toNanos(10)
        def future = new DeadlineRecordingFuture({ throw new AssertionError('Expired future must not be awaited') })
        admin.featuresFuture = future

        when:
        read(deadline)

        then:
        thrown(AdminReadDeadline.Exhausted)
        admin.requests*.operation == ['describeFeatures']
        future.waits.empty
    }

    void 'an in-flight response one nanosecond before expiry can use a sub-millisecond remaining wait'() {
        given:
        def deadline = budget(10)
        clock.now = deadline.deadlineNanos() - 8L
        def future = new DeadlineRecordingFuture<String>({ clock.now = deadline.deadlineNanos() - 1L; 'reply' })

        expect:
        deadline.await(future) == 'reply'
        future.waits == [8L]
    }

    void 'a value observed #late ns after the deadline cannot prove success'() {
        given:
        def deadline = budget(10)
        def future = new DeadlineRecordingFuture<String>({ clock.now = deadline.deadlineNanos() + late; 'reply' })

        when:
        deadline.await(future)

        then:
        thrown(AdminReadDeadline.Exhausted)
        future.waits == [MILLISECONDS.toNanos(10)]

        where:
        late << [0L, 1L]
    }

    void 'a real #category from #rpc retains its cause during terminal snapshot collection'() {
        given:
        def future = new KafkaFutureImpl()
        future.completeExceptionally(cause)
        if (rpc == 'describeFeatures') admin.featuresFuture = future
        else if (rpc == 'describeConfigs') admin.configFailure = cause
        else admin.directoryFutures[0] = future

        when:
        read(budget(10))

        then:
        def error = thrown(ExecutionException)
        error.cause.is(cause)
        AdminReassignmentExecutor.category(error) == category
        admin.writes == 0

        where:
        rpc                | cause                                                            | category
        'describeFeatures' | new org.apache.kafka.common.errors.TimeoutException('private')   | RPC_TIMEOUT
        'describeFeatures' | new ClusterAuthorizationException('private')                     | AUTHORIZATION
        'describeConfigs'  | new org.apache.kafka.common.errors.TimeoutException('private')   | RPC_TIMEOUT
        'describeLogDirs'  | new org.apache.kafka.common.errors.TimeoutException('private')   | RPC_TIMEOUT
    }

    void 'the effective deadline and remaining duration survive nanoTime wrapping'() {
        given:
        clock.now = Long.MAX_VALUE - MILLISECONDS.toNanos(2)
        long applyDeadline = clock.now + MILLISECONDS.toNanos(7)
        def deadline = AdminReadDeadline.forVerification(clock.&nanoTime as LongSupplier, applyDeadline, MILLISECONDS.toNanos(10))

        expect:
        deadline.deadlineNanos() == applyDeadline
        deadline.timeoutMs() == 7

        when:
        clock.now += MILLISECONDS.toNanos(3)

        then:
        clock.now < 0
        deadline.timeoutMs() == 4
    }

    void '100 ms polling is capped by the apply deadline shared by both partitions'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2], [1, 2, 3]], 2)
        trial.harness.rpcTimeoutMs = 10_000
        trial.harness.pollIntervalMs = 100
        trial.harness.timeoutMs = 135
        trial.waitThenConverge(Integer.MAX_VALUE)
        long start = trial.harness.clock.now

        when:
        trial.apply()

        then:
        def error = thrown(ReplicationExecutionException)
        error.phase == ReplicationExecutionException.Phase.VERIFICATION
        error.result.state() == ReassignmentExecutor.State.UNKNOWN
        error.result.partitions().values().every { it.submission() == ReassignmentExecutor.Submission.SENT
                && it.error() == DEADLINE_EXCEEDED }
        trial.recheckBudgets() == [135, 35]
        trial.harness.clock.now - start == MILLISECONDS.toNanos(135)
        trial.harness.clock.pauses == 2
        trial.harness.fake.writes.size() == 1
        trial.harness.fake.writes.first().size() == 2
    }

    private AdminReadDeadline budget(long millis) {
        long nanos = MILLISECONDS.toNanos(millis)
        AdminReadDeadline.forVerification(clock.&nanoTime as LongSupplier, clock.now + nanos, nanos)
    }

    private def read(AdminReadDeadline deadline) {
        ReplicationMetadataReader.readForVerification(admin.admin, ['orders'] as Set, [:], deadline)
    }
}

class DeadlineRecordingAdmin extends ConvergingMetadataAdmin {
    ExecutorClock clock
    boolean delayOnClockRead
    long dispatchNanos
    KafkaFutureImpl featuresFuture
    List<Map> requests = []

    @Override Object invoke(Object proxy, Method method, Object[] args) {
        if (!(args?.last() instanceof AbstractOptions)) return super.invoke(proxy, method, args)
        requests.add([operation: method.name, timeoutMs: args.last().timeoutMs(), sentAt: clock.now])
        clock.now += dispatchNanos
        if (method.name == 'describeFeatures' && featuresFuture != null) return new DescribeFeaturesResult(featuresFuture)
        super.invoke(proxy, method, args)
    }
}

class DeadlineRecordingFuture<T> extends KafkaFutureImpl<T> {
    final List<Long> waits = []
    private final Closure<T> reply

    DeadlineRecordingFuture(Closure<T> reply) {
        this.reply = reply
    }

    @Override T get(long timeout, TimeUnit unit) {
        long nanos = unit.toNanos(timeout)
        waits.add(nanos)
        reply.call(nanos)
    }
}
