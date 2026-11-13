// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*

class ReassignmentExecutorContractSpec extends Specification {
    private static TopicPartition partition(int id) { new TopicPartition('orders',id) }

    @Unroll
    void 'aggregate execution state #state never reports early completion'() {
        when:
        def result = new ExecutionResult([partition(0),partition(1)] as Set,
                [(partition(0)):new PartitionOutcome(State.COMPLETED,ErrorCategory.NONE),
                 (partition(1)):new PartitionOutcome(state,error)])

        then:
        result.state() == state
        result.partiallyCompleted() == (state != State.COMPLETED)

        where:
        state | error
        State.ACCEPTED | ErrorCategory.NONE
        State.RUNNING | ErrorCategory.NONE
        State.COMPLETED | ErrorCategory.NONE
        State.FAILED | ErrorCategory.BROKER_UNAVAILABLE
        State.UNKNOWN | ErrorCategory.TIMEOUT
    }

    void 'missing partition outcomes must be explicit UNKNOWN instead of absent success'() {
        when:
        new ExecutionResult([partition(0),partition(1)] as Set,
                [(partition(0)):new PartitionOutcome(State.COMPLETED,ErrorCategory.NONE)])

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    void 'inconsistent error classification #state #error is rejected'() {
        when:
        new PartitionOutcome(state,error)

        then:
        thrown(IllegalArgumentException)

        where:
        state | error
        State.COMPLETED | ErrorCategory.TIMEOUT
        State.ACCEPTED | ErrorCategory.AUTHORIZATION
        State.RUNNING | ErrorCategory.PARTIAL_FAILURE
        State.FAILED | ErrorCategory.NONE
        State.UNKNOWN | ErrorCategory.NONE
    }

    void 'request requires READY observations fresh permissions and a common positive deadline'() {
        given:
        def f = fixture()
        def ready = new ReplicationSafetyPolicy().evaluate('orders',f.plan,observed(f.base),permissions(),constraints())
        def operation = new PreparedOperation('orders',f.plan,ready)

        expect:
        new Request([operation],permissions(),100L,1000L).deadlineNanos() == 1000L

        when:
        new Request([operation],permissions(false,false,false),100L,1000L)

        then:
        thrown(IllegalArgumentException)

        when:
        new Request([operation],permissions(),100L,100L)

        then:
        thrown(IllegalArgumentException)
    }

    void 'BLOCKED or incomplete READY cannot become an executor request'() {
        given:
        def f = fixture()
        def blocked = new ReplicationSafetyPolicy().evaluate('orders',f.plan,f.base,permissions(),constraints())

        when:
        new PreparedOperation('orders',f.plan,blocked)

        then:
        thrown(IllegalArgumentException)

        when:
        new PreparedOperation('orders',f.plan,new ReplicationSafetyResult([],[],[]))

        then:
        thrown(IllegalArgumentException)
    }

    void 'executor contract remains an interface without default execution'() {
        expect:
        ReassignmentExecutor.isInterface()
        ReassignmentExecutor.declaredMethods*.name == ['execute']
    }
}
