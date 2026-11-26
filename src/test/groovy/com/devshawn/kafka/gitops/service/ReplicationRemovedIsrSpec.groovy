// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import org.apache.kafka.clients.admin.PartitionReassignment
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import spock.lang.Specification

import java.time.Duration
import java.util.function.LongSupplier

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.permissions
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.ErrorCategory.AUTHORIZATION
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.ErrorCategory.DEADLINE_EXCEEDED
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.ErrorCategory.STALE_PLAN

class ReplicationRemovedIsrSpec extends Specification {
    void 'normal decrease and increase use the original terminal observation without added safety reads'() {
        given:
        def trial = new RemovedIsrTrial(original, target)
        trial.afterExecution { complete -> trial.harness.fake.current = complete }

        expect:
        trial.apply().state() == State.COMPLETED
        trial.recheckBudgets().empty
        trial.harness.clock.pauses == 0
        trial.harness.fake.writes.size() == 1

        where:
        original    | target
        [[0, 1, 2]] | 2
        [[0, 1]]    | 3
    }

    void 'a decrease with #removed removed replicas converges after #reads fresh reads within the shared window'() {
        given:
        def trial = new RemovedIsrTrial([removed == 1 ? [0, 1, 2] : [0, 1, 2, 3]], 2)
        trial.waitThenConverge(reads)

        when:
        def result = trial.apply()

        then:
        result.state() == State.COMPLETED
        trial.recheckBudgets() == (0..<reads).collect { 20 - 5 * it }
        trial.harness.clock.pauses == reads - 1
        trial.harness.fake.writes.size() == 1
        trial.harness.fake.writes.first() == trial.harness.targets

        where:
        removed | reads
        1       | 1
        1       | 3
        2       | 2
    }

    void 'a smaller operation budget limits the entire terminal read sequence for both partitions'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2], [1, 2, 3]], 2)
        trial.harness.timeoutMs = budget
        trial.waitThenConverge(Integer.MAX_VALUE)
        long started = trial.harness.clock.now

        when:
        trial.apply()

        then:
        def error = thrown(ReplicationExecutionException)
        assertUnconfirmed(error, DEADLINE_EXCEEDED)
        trial.recheckBudgets() == budgets
        trial.harness.clock.now - started == Duration.ofMillis(elapsed).toNanos()
        trial.harness.fake.writes.size() == 1
        trial.harness.fake.writes.first().size() == 2

        where:
        budget | budgets         | elapsed
        7      | [7, 2]          | 7
        50     | [20, 15, 10, 5] | 20
    }

    void 'a safe final sample returned at the deadline cannot confirm success'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2]], 2)
        trial.afterExecution { complete ->
            trial.harness.fake.current = trial.extraIsr(complete)
            trial.harness.snapshotMutation = { ignored ->
                trial.harness.clock.now += Duration.ofMillis(20).toNanos()
                complete
            }
        }

        when:
        trial.apply()

        then:
        def error = thrown(ReplicationExecutionException)
        assertUnconfirmed(error, DEADLINE_EXCEEDED)
        trial.recheckBudgets() == [20]
        trial.harness.fake.writes.size() == 1
    }

    void 'the new terminal branch refuses #problem without another read or write'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2]], target)
        trial.afterExecution { complete ->
            def p = complete.topics().orders.partitions().first()
            def original = trial.harness.previous.values().first()
            def base = problem == 'increase' ? [3] : original
            List<Integer> isr = switch (problem) {
                case 'unknown ISR member' -> p.replicas() + [-1]
                case 'foreign ISR member' -> p.replicas() + [3]
                case 'missing target' -> base - p.replicas().last()
                case 'increase' -> p.replicas() + [9]
                default -> base
            }
            List<Integer> replicas = switch (problem) {
                case 'different assignment' -> [0, 3]
                case 'different order' -> p.replicas().reverse()
                default -> p.replicas()
            }
            def leader = problem == 'missing leader' ? OptionalInt.empty() : p.leader()
            trial.harness.fake.current = topic(complete, 'orders', [partitions:
                    [new Partition(0, replicas, leader, isr as Set)]])
            if (problem == 'active reassignment') {
                trial.harness.fake.active[new TopicPartition('orders', 0)] =
                        new PartitionReassignment(original, [], original - p.replicas())
            }
            if (problem == 'read failure') trial.harness.fake.readFailure = new ClusterAuthorizationException('private-error')
        }

        when:
        trial.apply()

        then:
        def error = thrown(ReplicationExecutionException)
        assertUnconfirmed(error, category)
        trial.recheckBudgets().empty
        trial.harness.clock.pauses == 0
        trial.harness.fake.writes.size() == 1

        where:
        problem                | target | category
        'unknown ISR member'   | 2      | STALE_PLAN
        'foreign ISR member'   | 2      | STALE_PLAN
        'missing target'       | 2      | STALE_PLAN
        'missing leader'       | 2      | STALE_PLAN
        'different assignment' | 2      | STALE_PLAN
        'different order'      | 2      | STALE_PLAN
        'active reassignment'  | 2      | STALE_PLAN
        'read failure'         | 2      | AUTHORIZATION
        'increase'             | 4      | STALE_PLAN
    }

    void 'fresh safety evidence catches #problem before further polling or any success'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2]], 2)
        trial.afterExecution { complete ->
            def skew = trial.extraIsr(complete)
            trial.harness.fake.current = skew
            int reads = 0
            trial.harness.snapshotMutation = { ignored ->
                if (++reads == 1) return skew
                switch (problem) {
                    case 'new rack': return broker(skew, 0, [rack: Optional.of('different-rack')])
                    case 'broker health': return broker(skew, 0, [reachable: UNAVAILABLE])
                    case 'min ISR changed': return topic(complete, 'orders', [minInSyncReplicas: OptionalInt.of(3)])
                    case 'min ISR unknown': return topic(complete, 'orders', [minInSyncReplicas: OptionalInt.empty()])
                    case 'active reassignment': return changed(skew, [reassignments: [new TopicPartition('orders', 0)] as Set])
                    case 'active reassignment unknown': return changed(skew, [reassignmentState: UNKNOWN])
                    case 'ELR':
                    case 'LastKnownELR':
                        def part = skew.topics().orders.partitions().first()
                        def elr = problem == 'ELR' ? [part.replicas().last()] : []
                        def lastKnown = problem == 'LastKnownELR' ? [part.replicas().last()] : []
                        return topic(skew, 'orders', [partitions: [new Partition(part.id(), part.replicas(), part.leader(),
                                part.isr(), Optional.of(elr), Optional.of(lastKnown))]])
                    case 'storage': return broker(skew, 0, [storage: UNKNOWN])
                    case 'API error': throw new ClusterAuthorizationException('private-read-response')
                    default: throw new AssertionError(problem)
                }
            }
        }

        when:
        trial.apply()

        then:
        def error = thrown(ReplicationExecutionException)
        assertUnconfirmed(error, category)
        trial.recheckBudgets() == [20, 15]
        trial.harness.clock.pauses == 1
        trial.harness.fake.writes.size() == 1

        where:
        problem                       | category
        'new rack'                    | STALE_PLAN
        'broker health'               | STALE_PLAN
        'min ISR changed'             | STALE_PLAN
        'min ISR unknown'             | STALE_PLAN
        'active reassignment'         | STALE_PLAN
        'active reassignment unknown' | STALE_PLAN
        'ELR'                         | STALE_PLAN
        'LastKnownELR'                | STALE_PLAN
        'storage'                     | STALE_PLAN
        'API error'                   | AUTHORIZATION
    }

    void 'all partitions must stabilize in a single fresh observation'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2], [1, 2, 3]], 2)
        trial.afterExecution { complete ->
            def first = trial.extraIsr(complete, 0)
            def second = trial.extraIsr(complete, 1)
            trial.harness.fake.current = first
            def observations = [first, second, complete].iterator()
            trial.harness.snapshotMutation = { ignored -> observations.next() }
        }

        when:
        def result = trial.apply()

        then:
        result.state() == State.COMPLETED
        result.partitions().size() == 2
        trial.recheckBudgets() == [20, 15, 10]
        trial.harness.fake.writes.size() == 1
    }

    void 'real metadata reader returns the fresh ISR when the known removal converges inside a snapshot'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2]], 2)
        def plan = trial.harness.request.operations().first().plan()
        def target = plan.assignments.first().targetReplicas
        def complete = snapshot([orders: [target]], [0: 'a', 1: 'b', 2: 'c'])
        def admin = new ConvergingMetadataAdmin(state: complete, lag: trial.extraIsr(complete), remaining: 1)

        when:
        def read = ReplicationMetadataReader.readForVerification(admin.admin, ['orders'] as Set, [orders: plan], metadataDeadline())

        then:
        read.topics().orders.partitions().first().isr() == target as Set
        admin.lagReads == 1
        admin.writes == 0
    }

    void 'real metadata reader refuses a foreign ISR member even when the second sample is already clean'() {
        given:
        def trial = new RemovedIsrTrial([[0, 1, 2]], 2)
        def plan = trial.harness.request.operations().first().plan()
        def target = plan.assignments.first().targetReplicas
        def complete = snapshot([orders: [target]], [0: 'a', 1: 'b', 2: 'c'])
        def lag = topic(complete, 'orders', [partitions:
                [new Partition(0, target, OptionalInt.of(target.first()), (target + [3]) as Set)]])
        def admin = new ConvergingMetadataAdmin(state: complete, lag: lag, remaining: 1)

        when:
        ReplicationMetadataReader.readForVerification(admin.admin, ['orders'] as Set, [orders: plan], metadataDeadline())

        then:
        def error = thrown(KafkaExecutionException)
        error.code == KafkaExecutionException.Code.METADATA_INVALID
        admin.lagReads == 1
        admin.writes == 0
    }

    private static AdminReadDeadline metadataDeadline() {
        def clock = new ExecutorClock()
        long window = Duration.ofSeconds(2).toNanos()
        AdminReadDeadline.forVerification(clock.&nanoTime as LongSupplier, clock.now + window, window)
    }

    private static void assertUnconfirmed(ReplicationExecutionException error, ErrorCategory category) {
        assert error.phase == ReplicationExecutionException.Phase.VERIFICATION
        assert error.result.state() == State.UNKNOWN
        assert error.result.partitions().values().every { p ->
            p.error() == category && p.submission() == Submission.SENT && p.lastKnownState() == State.COMPLETED
        }
        assert !error.message.contains('private-')
    }
}

class RemovedIsrTrial {
    final ReplicationApplicationTestHarness harness
    private int verificationStart

    RemovedIsrTrial(List<List<Integer>> original, int target) {
        harness = new ReplicationApplicationTestHarness([orders: original], [orders: target])
        harness.rpcTimeoutMs = 20
        harness.batchSize = 100
    }

    void afterExecution(Closure arrange) {
        harness.executeOverride = { engine, request ->
            def result = engine.execute(request)
            assert result.state() == State.COMPLETED
            verificationStart = harness.snapshotTimeouts.size()
            arrange(harness.fake.current)
            result
        }
    }

    void waitThenConverge(int reads) {
        afterExecution { complete ->
            def skew = extraIsr(complete)
            harness.fake.current = skew
            int next = 0
            harness.snapshotMutation = { ignored -> ++next < reads ? skew : complete }
        }
    }

    def apply() {
        def operation = harness.request.operations().first()
        def plan = new TopicPlan.Builder().setAction(UPDATE).setName(operation.topicName()).setReplicationPlan(operation.plan()).build()
        harness.application().apply([plan], permissions())
    }

    List<Integer> recheckBudgets() { harness.snapshotTimeouts.drop(verificationStart) }

    def extraIsr(snapshot, int partition = 0) {
        def partitions = snapshot.topics().orders.partitions().collect { part ->
            if (part.id() != partition) return part
            def previous = harness.previous[new TopicPartition('orders', part.id())]
            new Partition(part.id(), part.replicas(), part.leader(), previous as Set)
        }
        topic(snapshot, 'orders', [partitions: partitions])
    }
}
