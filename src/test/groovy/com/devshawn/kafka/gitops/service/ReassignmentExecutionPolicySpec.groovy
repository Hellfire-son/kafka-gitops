package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Status.*
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Reason.*

class ReassignmentExecutionPolicySpec extends Specification {
    def policy = new ReplicationSafetyPolicy()

    void 'completed scope validates actual target and leaves ordinary preflight strict'() {
        given:
        def f = fixture()
        def target = f.plan.assignments.first().targetReplicas
        def current = topic(f.base,'orders',[partitions:[new Partition(0,target,OptionalInt.of(0),target as Set),
                f.base.topics().orders.partitions().last()]])
        current = observed(current)

        expect:
        policy.evaluate('orders',f.plan,current,permissions(),constraints()).reasons()*.code().contains(PREVIOUS_REPLICAS_CHANGED)
        policy.evaluateForExecution('orders',f.plan,current,permissions(),constraints(),[0] as Set).status() == READY
    }

    @Unroll
    void 'completed scope cannot manufacture READY for #description'() {
        given:
        def f = fixture()

        when:
        def result = policy.evaluateForExecution('orders',f.plan,observed(mutation(f.base)),permissions(),constraints(),completed)

        then:
        result.status() == BLOCKED
        result.reasons()*.code().contains(reason)

        where:
        description | mutation | completed | reason
        'original assignment declared completed' | { it } | [0] as Set | PREVIOUS_REPLICAS_CHANGED
        'invalid partition scope' | { it } | [9] as Set | STRUCTURE_INVALID
        'missing partition scope' | { it } | null | STRUCTURE_INVALID
        'unknown fencing' | { broker(it,2,[unfenced:UNKNOWN]) } | [] as Set | BROKER_FENCING_UNKNOWN
        'no actual target ISR' | { topic(it,'orders',[partitions:[new Partition(0,[0,2],OptionalInt.of(0),[0] as Set),it.topics().orders.partitions().last()]]) } | [0] as Set | ISR_INCOMPLETE
    }
}
