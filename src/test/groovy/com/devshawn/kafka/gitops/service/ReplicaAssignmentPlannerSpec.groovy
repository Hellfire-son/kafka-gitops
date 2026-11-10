package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.mapper
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*

class ReplicaAssignmentPlannerSpec extends Specification {
    def planner = new ReplicaAssignmentPlanner()

    @Unroll
    void 'plans #previous -> #rf preserving order and leader'() {
        given:
        def s = snapshot([orders:previous])

        when:
        def plan = planner.plan(s, [orders:rf], RACK_AWARE, true, false).orders

        then:
        plan.assignments*.previousReplicas == previous
        plan.assignments.every { a -> a.targetReplicas.size() == rf && a.targetReplicas.toSet().size() == rf }
        plan.assignments.every { a -> a.targetReplicas.contains(previous[a.partition].first()) }
        plan.assignments.every { a ->
            rf >= a.previousReplicas.size() ? a.targetReplicas.take(a.previousReplicas.size()) == a.previousReplicas :
                    a.previousReplicas.findAll { a.targetReplicas.contains(it) } == a.targetReplicas
        }
        plan.requiresDecreaseApproval() == previous.any { it.size() > rf }
        ReplicationPlanValidation.validate(plan) == null

        where:
        previous               | rf
        [[0], [1]]             | 2
        [[0], [1]]             | 3
        [[0,1], [1,0]]         | 3
        [[0,1,2], [1,2,0]]     | 2
        [[0,1,2], [1,2,0]]     | 1
        [[0,1], [1,2,3]]       | 3
        [[0], [1,2,3]]         | 2
    }

    void 'unchanged RF produces no operation and no hidden rebalance'() {
        expect:
        planner.plan(snapshot([orders:[[2,0],[1,3]]]), [orders:2], RACK_AWARE, false, true).isEmpty()
    }

    void 'unchanged partition retains exact IDs and order in mixed RF topic'() {
        when:
        def plan = planner.plan(snapshot([orders:[[2,0],[1]]]), [orders:2], RACK_AWARE, false, false).orders

        then:
        plan.assignments.first().targetReplicas == [2,0]
        plan.assignments.last().targetReplicas.first() == 1
    }

    void 'decrease retains live leader even when not the preferred first replica'() {
        given:
        def s = snapshot([orders:[[0,1,2]]])
        s = topic(s, 'orders', [partitions:[new Partition(0, [0,1,2], OptionalInt.of(2), [0,1,2] as Set)]])

        expect:
        planner.plan(s, [orders:1], RACK_AWARE, true, false).orders.assignments.first().targetReplicas == [2]
        planner.plan(s, [orders:2], RACK_AWARE, true, false).orders.assignments.first().targetReplicas == [0,2]
    }

    void 'rack diversity precedes load and broker ID with repeated racks'() {
        given:
        def s = snapshot([orders:[[0]]], [0:'a',1:'a',2:'b',3:'b',4:'c'])
        s = changed(s, [replicaCounts:[0:1L,1:0L,2:5L,3:5L,4:9L]])

        expect:
        planner.plan(s, [orders:3], RACK_AWARE, false, false).orders.assignments.first().targetReplicas == [0,2,4]
        planner.plan(s, [orders:3], BROKER_ONLY, false, false).orders.assignments.first().targetReplicas == [0,1,2]
    }

    void 'RF may exceed number of racks while spreading across actual fault domains'() {
        expect:
        planner.plan(snapshot([orders:[[0]]], [0:'a',1:'a',2:'b',3:'b']), [orders:4], RACK_AWARE, false, false)
                .orders.assignments.first().targetReplicas == [0,2,1,3]
    }

    void 'projected additions are balanced across partitions and topics in stable name order'() {
        given:
        def s = snapshot([zeta:Collections.nCopies(15,[0]), alpha:Collections.nCopies(15,[0])])

        when:
        def plans = planner.plan(s, [zeta:2,alpha:2], BROKER_ONLY, false, false)
        def counts = plans.values().collectMany { it.assignments }.collect { it.targetReplicas.last() }.countBy { it }

        then:
        plans.keySet().toList() == ['alpha','zeta']
        counts == [1:10,2:10,3:10]
        plans.alpha.assignments.first().targetReplicas == [0,1]
        plans.zeta.assignments.first().targetReplicas == [0,1]
    }

    void 'map set and partition iteration order cannot change calculated JSON'() {
        given:
        def original = snapshot([zeta:[[0],[1]],alpha:[[2],[3]]])
        def expected = mapper().writeValueAsString(planner.plan(original, [alpha:3,zeta:3], RACK_AWARE, false, false))

        expect:
        (0..<12).every { iteration ->
            def ids = (0..3).toList(); Collections.rotate(ids, iteration)
            def names = iteration % 2 ? ['zeta','alpha'] : ['alpha','zeta']
            def topics = names.collectEntries { name ->
                def t = original.topics()[name]
                [(name):new Topic(t.topicId(), iteration % 2 ? t.partitions().reverse() : t.partitions(), t.minInSyncReplicas())]
            }
            def s = changed(original, [brokers:ids.collectEntries { [(it):original.brokers()[it]] },topics:topics,
                    replicaCounts:ids.collectEntries { [(it):original.replicaCounts()[it]] }])
            mapper().writeValueAsString(planner.plan(s, names.collectEntries { [(it):3] }, RACK_AWARE, false, false)) == expected
        }
    }

    @Unroll
    void 'ineligible new broker is excluded: #field #state'() {
        given:
        def s = broker(snapshot([orders:[[0]]]), 1, [(field):state])

        when:
        def plan = planner.plan(s, [orders:2], BROKER_ONLY, false, false).orders

        then:
        plan.assignments.first().targetReplicas == [0,2]
        !plan.brokerSnapshot*.id.contains(1)

        where:
        field | state
        'reachable' | UNAVAILABLE
        'reachable' | UNKNOWN
        'unfenced' | UNAVAILABLE
        'storage' | UNAVAILABLE
        'storage' | UNKNOWN
        'uncordoned' | UNAVAILABLE
        'uncordoned' | UNKNOWN
        'minimumUsableBytes' | OptionalLong.empty()
        'minimumUsableBytes' | OptionalLong.of(0)
    }

    @Unroll
    void 'blocks unknown or unsafe planning condition #description'() {
        given:
        def s = mutation(snapshot([orders:[[0,1,2]]]))

        when:
        planner.plan(s, [orders:rf], RACK_AWARE, allow, noDelete)

        then:
        def error = thrown(ValidationException)
        error.message.contains(message)

        where:
        description | mutation | rf | allow | noDelete | message
        'missing decrease permission' | { it } | 2 | false | false | '--allow-replication-decrease'
        'no-delete overrides permission' | { it } | 2 | true | true | '--no-delete'
        'unknown min ISR' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.empty()]) } | 2 | true | false | 'UNKNOWN'
        'min ISR above target' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(3)]) } | 2 | true | false | 'min.insync'
        'unknown reassignment' | { changed(it,[reassignmentState:UNKNOWN]) } | 2 | true | false | 'UNKNOWN'
        'active reassignment' | { changed(it,[reassignments:[new TopicPartition('orders',0)] as Set]) } | 2 | true | false | 'active reassignment'
        'unknown load' | { changed(it,[loadState:UNKNOWN]) } | 2 | true | false | 'UNKNOWN'
        'unknown original broker' | { broker(it,0,[reachable:UNKNOWN]) } | 2 | true | false | 'original replica'
        'unavailable original broker' | { broker(it,0,[reachable:UNAVAILABLE]) } | 2 | true | false | 'original replica'
        'missing original broker' | { changed(it,[brokers:it.brokers().findAll { k,v -> k != 0 }]) } | 2 | true | false | 'original replica'
        'missing load entry' | { changed(it,[replicaCounts:[:]]) } | 2 | true | false | 'incomplete'
        'understated load' | { changed(it,[replicaCounts:[0:0L,1:1L,2:1L,3:0L]]) } | 2 | true | false | 'understates'
        'insufficient brokers' | { it } | 5 | false | false | 'Too few'
        'zero target' | { it } | 0 | false | false | '1..32767'
        'negative target' | { it } | -1 | false | false | '1..32767'
        'too large target' | { it } | 32768 | false | false | '1..32767'
    }

    @Unroll
    void 'degraded partition blocks #caseName'() {
        given:
        def s = topic(snapshot([orders:[[0,1,2]]]), 'orders', [partitions:[partition]])

        when:
        planner.plan(s, [orders:2], RACK_AWARE, true, false)

        then:
        thrown(ValidationException)

        where:
        caseName | partition
        'missing leader' | new Partition(0,[0,1,2],OptionalInt.empty(),[0,1,2] as Set)
        'leader outside replicas' | new Partition(0,[0,1,2],OptionalInt.of(3),[0,1,2] as Set)
        'incomplete ISR' | new Partition(0,[0,1,2],OptionalInt.of(0),[0,1] as Set)
        'empty ISR' | new Partition(0,[0,1,2],OptionalInt.of(0),[] as Set)
        'extra ISR broker' | new Partition(0,[0,1,2],OptionalInt.of(0),[0,1,2,3] as Set)
        'duplicate broker' | new Partition(0,[0,0,2],OptionalInt.of(0),[0,2] as Set)
        'missing partition' | new Partition(1,[0,1,2],OptionalInt.of(0),[0,1,2] as Set)
    }

    @Unroll
    void 'partial or absent racks never silently change policy missing=#missing'() {
        given:
        def s = snapshot([orders:[[0]]], missing ? [0:'a',1:null,2:'c'] : [0:null,1:null,2:null])

        when:
        planner.plan(s, [orders:2], RACK_AWARE, false, false)

        then:
        def error = thrown(ValidationException)
        error.message.contains('BROKER_ONLY explicitly')

        when:
        def plan = planner.plan(s, [orders:2], BROKER_ONLY, false, false).orders

        then:
        plan.rackPolicy == BROKER_ONLY
        plan.assignments.first().targetReplicas == [0,1]

        where:
        missing << [false,true]
    }

    void 'removing replicas updates projected load before planning the next topic'() {
        given:
        def s = snapshot([alpha:[[0,1,2]], zeta:[[3]]])

        when:
        def plans = planner.plan(s, [zeta:2,alpha:1], BROKER_ONLY, true, false)

        then:
        plans.alpha.assignments.first().targetReplicas == [0]
        plans.zeta.assignments.first().targetReplicas == [3,1]
    }

    void 'UNKNOWN broker reachability cannot be used when there is no verified alternative'() {
        given:
        def s = broker(snapshot([orders:[[0]]],[0:'a',1:'b']),1,[reachable:UNKNOWN])

        when:
        planner.plan(s,[orders:2],RACK_AWARE,false,false)

        then:
        def error = thrown(ValidationException)
        error.message.contains('Too few verified eligible brokers')
    }
}
