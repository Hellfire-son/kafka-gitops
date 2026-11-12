package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.Admission
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.*
import static com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Reason.*

class ReplicationSafetyPolicySpec extends Specification {
    def policy = new ReplicationSafetyPolicy()

    @Unroll
    void 'fully attested #previous -> #target can be READY without being executable'() {
        given:
        def f = fixture(previous,target)

        when:
        def result = policy.evaluate('orders',f.plan,observed(f.base),permissions(),constraints())

        then:
        result.status() == Status.READY
        result.reasons().isEmpty()
        result.operations().every { it.status() == Status.READY }
        result.warnings().contains(Warning.NON_ATOMIC_SNAPSHOT)

        where:
        previous               | target
        [[0],[1]]              | 2
        [[0],[1]]              | 3
        [[0,1],[1,0]]          | 3
        [[0,1,2],[1,2,0]]      | 2
        [[0,1,2],[1,2,0]]      | 1
        [[0],[1,2,3]]          | 2
    }

    @Unroll
    void 'blocks changed metadata #description with typed reason #code'() {
        given:
        def f = fixture()
        def current = observed(mutation(f.base))

        when:
        def result = policy.evaluate('orders',f.plan,current,permissions(),constraints())

        then:
        result.status() == Status.BLOCKED
        result.reasons()*.code().contains(code)
        result.operations().every { it.status() == Status.BLOCKED }

        where:
        description | mutation | code
        'cluster ID' | { changed(it,[clusterId:'private-id']) } | CLUSTER_ID_CHANGED
        'missing topic' | { changed(it,[topics:[:]]) } | TOPIC_NOT_FOUND
        'topic UUID' | { topic(it,'orders',[topicId:'private-topic-id']) } | TOPIC_ID_CHANGED
        'partition count' | { topic(it,'orders',[partitions:it.topics().orders.partitions().take(1)]) } | PARTITION_COUNT_CHANGED
        'duplicate partition' | { topic(it,'orders',[partitions:[it.topics().orders.partitions().first(),it.topics().orders.partitions().first()]]) } | PARTITION_COUNT_CHANGED
        'replica composition' | { topic(it,'orders',[partitions:[new Partition(0,[3],OptionalInt.of(3),[3] as Set),it.topics().orders.partitions().last()]]) } | PREVIOUS_REPLICAS_CHANGED
        'unknown leader' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.empty(),[0] as Set),it.topics().orders.partitions().last()]]) } | LEADER_UNKNOWN
        'invalid leader' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.of(2),[0] as Set),it.topics().orders.partitions().last()]]) } | LEADER_NOT_IN_ISR
        'ISR loss' | { topic(it,'orders',[partitions:[new Partition(0,[0],OptionalInt.of(0),[] as Set),it.topics().orders.partitions().last()]]) } | ISR_INCOMPLETE
        'unknown min ISR' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.empty()]) } | MIN_ISR_UNKNOWN
        'min ISR drift' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(2)]) } | MIN_ISR_CHANGED
        'target below min ISR' | { topic(it,'orders',[minInSyncReplicas:OptionalInt.of(3)]) } | TARGET_BELOW_MIN_ISR
        'unknown reassignment' | { changed(it,[reassignmentState:UNKNOWN]) } | REASSIGNMENT_UNKNOWN
        'active reassignment' | { changed(it,[reassignments:[new TopicPartition('orders',0)] as Set]) } | ACTIVE_REASSIGNMENT
        'broker missing' | { changed(it,[brokers:it.brokers().findAll { id,b -> id != 2 }]) } | TOPOLOGY_CHANGED
        'broker offline' | { broker(it,2,[reachable:UNAVAILABLE]) } | BROKER_OFFLINE
        'unknown health' | { broker(it,2,[reachable:UNKNOWN]) } | BROKER_HEALTH_UNKNOWN
        'fenced broker' | { broker(it,2,[unfenced:UNAVAILABLE]) } | BROKER_FENCED
        'unknown fencing' | { broker(it,2,[unfenced:UNKNOWN]) } | BROKER_FENCING_UNKNOWN
        'storage offline' | { broker(it,2,[storage:UNAVAILABLE]) } | STORAGE_UNAVAILABLE
        'storage unknown' | { broker(it,2,[storage:UNKNOWN]) } | STORAGE_UNKNOWN
        'cordoned target' | { broker(it,2,[uncordoned:UNAVAILABLE]) } | BROKER_CORDONED
        'cordoning unknown' | { broker(it,2,[uncordoned:UNKNOWN]) } | CORDONING_UNKNOWN
        'rack lost' | { broker(it,2,[rack:Optional.empty()]) } | RACK_METADATA_UNKNOWN
        'rack changed' | { broker(it,2,[rack:Optional.of('different-rack')]) } | TOPOLOGY_CHANGED
        'full disk' | { broker(it,2,[minimumUsableBytes:OptionalLong.of(0)]) } | DISK_INSUFFICIENT
        'unknown usable bytes' | { broker(it,2,[minimumUsableBytes:OptionalLong.empty()]) } | DISK_CAPACITY_UNKNOWN
    }

    @Unroll
    void 'unknown or changed evidence blocks #description'() {
        given:
        def f = fixture()

        when:
        def result = policy.evaluate('orders',f.plan,observed(f.base,override),permissions(),constraints())

        then:
        result.status() == Status.BLOCKED
        result.reasons()*.code().contains(code)

        where:
        description | override | code
        'no timestamp' | [completedAtNanos:OptionalLong.empty()] | SNAPSHOT_NOT_FRESH
        'ancient timestamp' | [completedAtNanos:OptionalLong.of(1L)] | SNAPSHOT_NOT_FRESH
        'ELR unknown' | [elrMode:com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN] | ELR_MODE_UNKNOWN
        'ELR enabled' | [elrMode:ENABLED] | ELR_UNSUPPORTED
        'feature drift' | [featuresStable:UNAVAILABLE] | FEATURES_CHANGED
        'feature stability unknown' | [featuresStable:UNKNOWN] | ELR_MODE_UNKNOWN
        'configs drift' | [configsStable:[orders:UNAVAILABLE]] | CONFIG_CHANGED
        'configs unavailable' | [configsStable:[:]] | CONFIG_STATE_UNKNOWN
        'cordoning changed' | [cordoningStable:[2:UNAVAILABLE]] | CORDONING_CHANGED
        'cordoning stability unknown' | [cordoningStable:[:]] | CORDONING_UNKNOWN
        'no placement proof' | [admissions:[:]] | PLACEMENT_UNKNOWN
        'placement forbidden' | [admissions:[2:new Admission(UNAVAILABLE,VERIFIED)]] | PLACEMENT_FORBIDDEN
        'no disk reserve proof' | [admissions:[2:new Admission(VERIFIED,UNKNOWN)]] | DISK_CAPACITY_UNKNOWN
        'insufficient disk reserve' | [admissions:[2:new Admission(VERIFIED,UNAVAILABLE)]] | DISK_INSUFFICIENT
    }

    @Unroll
    void 'permissions manage=#manage decrease=#allow noDelete=#noDelete are independent of JSON'() {
        given:
        def f = fixture([[0,1,2]],2)

        when:
        def result = policy.evaluate('orders',f.plan,observed(f.base),permissions(manage,allow,noDelete),constraints())

        then:
        result.status() == Status.BLOCKED
        result.reasons()*.code().contains(code)

        where:
        manage | allow | noDelete | code
        false | false | false | MANAGE_REPLICATION_REQUIRED
        false | true  | false | INVALID_PERMISSION_COMBINATION
        true  | false | false | DECREASE_PERMISSION_REQUIRED
        true  | true  | true  | NO_DELETE
    }

    void 'reordered replica IDs are drift even with the same set'() {
        given:
        def f = fixture([[0,1]],3)
        def s = topic(f.base,'orders',[partitions:[new Partition(0,[1,0],OptionalInt.of(0),[0,1] as Set)]])

        expect:
        policy.evaluate('orders',f.plan,observed(s),permissions(),constraints()).reasons()*.code().contains(PREVIOUS_REPLICAS_CHANGED)
    }

    void 'changed current leader must remain after decrease'() {
        given:
        def f = fixture([[0,1,2]],1)
        def s = topic(f.base,'orders',[partitions:[new Partition(0,[0,1,2],OptionalInt.of(2),[0,1,2] as Set)]])

        expect:
        policy.evaluate('orders',f.plan,observed(s),permissions(),constraints()).reasons()*.code().contains(LEADER_NOT_RETAINED)
    }

    @Unroll
    void 'context conflict config=#config partitions=#partition blocks the operation'() {
        given:
        def f = fixture()

        expect:
        policy.evaluate('orders',f.plan,observed(f.base),permissions(),constraints(config,partition)).reasons()*.code().contains(code)

        where:
        config | partition | code
        true | false | CONFIG_CONFLICT
        false | true | PARTITION_CONFLICT
    }

    void 'planning snapshot is not fresh execution evidence and missing metadata fails closed'() {
        given:
        def f = fixture()

        expect:
        policy.evaluate('orders',f.plan,f.base,permissions(),constraints()).status() == Status.BLOCKED
        policy.evaluate('orders',f.plan,null,permissions(),constraints()).reasons()*.code().contains(METADATA_UNAVAILABLE)
    }

    void 'invalid plan structure is a typed rejection without echoing input'() {
        given:
        def f = fixture()
        def invalid = new TopicReplicationPlan.Builder().mergeFrom(f.plan).setClusterId('private-invalid-secret').build()

        when:
        def result = policy.evaluate('orders',invalid,observed(f.base),permissions(),constraints())

        then:
        result.reasons()*.code() == [STRUCTURE_INVALID]
        !result.toString().contains('private-')
    }

    void 'decrease cannot retain a replica outside ISR and blocks every mixed RF partition'() {
        given:
        def f = fixture([[0],[1,2,3]],2)
        def s = topic(f.base,'orders',[partitions:[f.base.topics().orders.partitions().first(),
                new Partition(1,[1,2,3],OptionalInt.of(1),[1] as Set)]])

        when:
        def result = policy.evaluate('orders',f.plan,observed(s),permissions(),constraints())

        then:
        result.status() == Status.BLOCKED
        result.reasons()*.code().contains(TARGET_NOT_IN_ISR)
        result.operations()*.operation().toSet() == [Operation.INCREASE,Operation.DECREASE] as Set
        result.operations().every { it.status() == Status.BLOCKED }
    }

    void 'broker-only is explicit and still requires all health and admission checks'() {
        given:
        def base = snapshot([orders:[[0]]],[0:null,1:null,2:null])
        def plan = new ReplicaAssignmentPlanner().plan(base,[orders:2],
                com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.BROKER_ONLY,false,false).orders

        when:
        def ready = policy.evaluate('orders',plan,observed(base),permissions(),constraints())

        then:
        ready.status() == Status.READY
        ready.warnings().contains(Warning.BROKER_ONLY)

        when:
        def blocked = policy.evaluate('orders',plan,observed(broker(base,1,[reachable:UNKNOWN])),permissions(),constraints())

        then:
        blocked.reasons()*.code().contains(BROKER_HEALTH_UNKNOWN)
    }

    void 'a new eligible broker invalidates the pinned placement topology'() {
        given:
        def f = fixture()
        def brokers = new HashMap<>(f.base.brokers())
        brokers[4] = new Broker(4,Optional.of('e'),VERIFIED,VERIFIED,VERIFIED,VERIFIED,OptionalLong.of(1000L))

        expect:
        policy.evaluate('orders',f.plan,observed(changed(f.base,[brokers:brokers])),permissions(),constraints())
                .reasons()*.code().contains(TOPOLOGY_CHANGED)
    }

    void 'rack policy rejects avoidable co-location after a decrease'() {
        given:
        def base = snapshot([orders:[[0,1,2]]],[0:'a',1:'a',2:'b'])
        def valid = new ReplicaAssignmentPlanner().plan(base,[orders:2],
                com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE,true,false).orders
        def invalid = new TopicReplicationPlan.Builder().mergeFrom(valid).clearAssignments().addAssignments(
                com.devshawn.kafka.gitops.ReplicationTestSupport.assignment(0,[0,1,2],[0,1])).build()

        expect:
        policy.evaluate('orders',invalid,observed(base),permissions(),constraints()).reasons()*.code().contains(RACK_POLICY_VIOLATION)
    }

    void 'READY is not possible for a cached snapshot predating the requested fresh read'() {
        given:
        def f = fixture()
        def s = observed(f.base)
        long now = System.nanoTime()
        def after = new ReplicationSafetyPolicy.Constraints(false,false,now,now,java.time.Duration.ofSeconds(30))

        expect:
        policy.evaluate('orders',f.plan,s,permissions(),after).reasons()*.code().contains(SNAPSHOT_NOT_FRESH)
    }
}
