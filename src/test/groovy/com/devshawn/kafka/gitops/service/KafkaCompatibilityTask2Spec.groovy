// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult
import com.devshawn.kafka.gitops.enums.PlanAction
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy
import com.devshawn.kafka.gitops.exception.ValidationException
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import spock.lang.Specification

import java.lang.reflect.Method

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.VERIFIED
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.Capability.*
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.State.*

class KafkaCompatibilityTask2Spec extends Specification {
    Task2ReadOnlyAdmin fake = new Task2ReadOnlyAdmin()
    def service() {
        new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
    }

    def read(boolean preflight=false) { preflight ? service().readReplicationPreflightSnapshot(['acl-test'] as Set) : service().readReplicationSnapshot(['acl-test'] as Set) }

    void 'two responding brokers without evidence do not prove placement and expose the exact missing capability'() {
        given:
        fake.feature=[failure:true]

        when:
        def observed=read()
        new ReplicaAssignmentPlanner().plan(observed,['acl-test':2],RackAwarenessPolicy.RACK_AWARE,true,false)

        then:
        thrown(ValidationException)
        observed.brokers().size()==2
        observed.brokers()[1].storage()==VERIFIED
        observed.brokers()[1].minimumUsableBytes().empty
        observed.brokers()[1].capabilities()[CORDONING].state()==UNKNOWN
        observed.brokers()[1].capabilities()[CORDONING].reason()==CapabilityAssessment.Reason.CAPABILITY_NOT_ESTABLISHED
        !observed.toString().contains('/synthetic-log')
        !fake.calls.contains('alterPartitionReassignments')
    }

    void 'old feature bound makes cordoning inapplicable and old log dirs do not fabricate capacity'() {
        when:
        def observed=read()
        def plan=new ReplicaAssignmentPlanner().plan(observed,['acl-test':2],RackAwarenessPolicy.RACK_AWARE,true,false)['acl-test']

        then:
        observed.brokers().values().every { it.capabilities()[CORDONING].state()==NOT_APPLICABLE && it.isPreviewCandidate() }
        observed.brokers().values().every { it.minimumUsableBytes().empty && it.capabilities()[DISK_CAPACITY].state()==UNKNOWN }
        observed.replicaCounts()==[0:4L,1:0L]
        plan.assignments*.previousReplicas==[[0],[0],[0],[0]]
        plan.assignments*.targetReplicas==[[0,1],[0,1],[0,1],[0,1]]
        !fake.calls.contains('alterPartitionReassignments')
    }

    void 'fresh critical invariants can pass with explicit noncritical capacity and admission warnings'() {
        given:
        long started=System.nanoTime()
        def observed=read(true)
        def rf=new ReplicaAssignmentPlanner().plan(observed,['acl-test':2],RackAwarenessPolicy.RACK_AWARE,true,false)['acl-test']
        def topic=new TopicPlan.Builder().setName('acl-test').setAction(PlanAction.UPDATE).setReplicationPlan(rf).build()

        when:
        def result=new ReplicationSafetyPolicy().evaluate('acl-test',rf,observed,new ReplicationSafetyPolicy.Permissions(true,false,false),
                ReplicationSafetyPolicy.Constraints.forTopic(topic,started,System.nanoTime()))

        then:
        observed.safety().elrMode()==ReplicationSafetyEvidence.ElrMode.NOT_APPLICABLE
        result.status()==ReplicationSafetyResult.Status.READY
        result.warnings().contains(ReplicationSafetyResult.Warning.DISK_RESERVE_NOT_PROVEN)
        result.warnings().contains(ReplicationSafetyResult.Warning.PLACEMENT_ADMISSION_NOT_PROVEN)
        !fake.calls.contains('alterPartitionReassignments')
    }

    void 'absence of cordoning on a capable cluster is UNKNOWN rather than NOT_APPLICABLE'() {
        given:
        fake.feature=[finalized:['metadata.version':new FinalizedVersionRange((short)30,(short)30)],
                      supported:['metadata.version':new SupportedVersionRange((short)7,(short)33)]]

        expect:
        read().brokers().values().every { it.capabilities()[CORDONING].state()==UNKNOWN && !it.isPreviewCandidate() }
    }

    void 'explicit dangerous cordoning always wins over an older feature-level claim'() {
        given:
        fake.brokerEntries[1]=[ReplicationMetadataSpec.entry('cordoned.log.dirs','/synthetic-log/cordoned')]

        expect:
        read().brokers()[1].capabilities()[CORDONING].state()==UNSAFE
        !read().brokers()[1].isPreviewCandidate()
    }

    void 'positive ZooKeeper properties establish inapplicability without guessing a release from ApiVersions'() {
        given:
        fake.feature=[failure:true]
        fake.brokerEntries=(0..1).collectEntries { [(it):[ReplicationMetadataSpec.entry('process.roles',''),ReplicationMetadataSpec.entry('zookeeper.connect','unused-zk.invalid:1')]] }

        when:
        def observed=read(true)

        then:
        observed.brokers().values().every { it.capabilities()[CORDONING].state()==NOT_APPLICABLE }
        observed.safety().elrMode()==ReplicationSafetyEvidence.ElrMode.NOT_APPLICABLE
        observed.safety().featuresStable()==VERIFIED
        !observed.toString().contains('unused-zk')
    }

    void 'known storage failure and missing min ISR and degraded ISR each remain hard blockers: #condition'() {
        given:
        if(condition=='storage') fake.directoryFutures[1]=KafkaFuture.completedFuture(['/synthetic-log':new LogDirDescription(new org.apache.kafka.common.errors.KafkaStorageException('private-error'),[:])])
        if(condition=='min-isr') fake.topicEntries=[]
        if(condition=='isr') {
            def t=fake.state.topics()['acl-test']
            fake.state=topic(fake.state,'acl-test',[partitions:t.partitions().collect { new ReplicationClusterSnapshot.Partition(it.id(),it.replicas(),it.leader(),[] as Set) }])
        }

        when:
        new ReplicaAssignmentPlanner().plan(read(),['acl-test':2],RackAwarenessPolicy.RACK_AWARE,true,false)

        then:
        thrown(ValidationException)
        !fake.calls.contains('alterPartitionReassignments')

        where:
        condition << ['storage','min-isr','isr']
    }

    void 'missing rack is not invented and requires an explicit broker-only policy'() {
        given:
        fake.state=snapshot(['acl-test':[[0],[0],[0],[0]]],[0:'1',1:null])

        when:
        new ReplicaAssignmentPlanner().plan(read(),['acl-test':2],RackAwarenessPolicy.RACK_AWARE,true,false)

        then:
        thrown(ValidationException)

        when:
        def plans=new ReplicaAssignmentPlanner().plan(read(),['acl-test':2],RackAwarenessPolicy.BROKER_ONLY,true,false)

        then:
        plans['acl-test'].assignments*.targetReplicas==[[0,1],[0,1],[0,1],[0,1]]
        plans['acl-test'].brokerSnapshot.find { it.id==1 }.rack.empty
    }
}

class Task2ReadOnlyAdmin extends PreflightReadOnlyAdmin {
    Task2ReadOnlyAdmin() {
        state=snapshot(['acl-test':[[0],[0],[0],[0]]],[0:'1',1:'2'])
        brokerEntries=[0:[],1:[]]
        feature=[finalized:['metadata.version':new FinalizedVersionRange((short)4,(short)4)],
                 supported:['metadata.version':new SupportedVersionRange((short)1,(short)4)]]
        directoryFutures=[0:KafkaFuture.completedFuture(['/synthetic-log/0':new LogDirDescription(null,
            (0..3).collectEntries { [(new TopicPartition('acl-test',it)):new ReplicaInfo(100L,0L,false)] })]),
            1:KafkaFuture.completedFuture(['/synthetic-log/1':new LogDirDescription(null,[:])])]
    }
}

/** In-memory execution fixture; the production binding still uses its real reader/policy/executor. */
class Task2ExecutionAdmin extends Task2ReadOnlyAdmin {
    int writes
    Task2ExecutionAdmin() { maximumTimeoutMs=10000; directoryFutures=[:] }
    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if(method.name=='describeFeatures') {
            // The inherited fixture deliberately bounds read-only unit calls to two seconds.
            args[0].timeoutMs(Math.min(2000,args[0].timeoutMs()))
        }
        if(method.name=='alterPartitionReassignments') {
            calls.add(method.name); writes++
            Map futures=[:]
            args[0].each { key,assignment ->
                assert assignment.present
                def replicas=assignment.get().targetReplicas()
                def previous=state.topics()[key.topic()]
                def partitions=previous.partitions().collect { p -> p.id()==key.partition()
                        ? new ReplicationClusterSnapshot.Partition(p.id(),replicas,p.leader(),replicas as Set) : p }
                state=topic(state,key.topic(),[partitions:partitions])
                futures[key]=KafkaFuture.completedFuture(null)
            }
            return new AlterPartitionReassignmentsResult(futures)
        }
        super.invoke(proxy,method,args)
    }
}
