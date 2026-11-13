// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import org.apache.kafka.clients.admin.*
import spock.lang.Specification
import java.lang.reflect.Method
import java.time.Duration
import java.util.function.BiFunction
import java.util.function.Supplier

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.permissions
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE

class ReplicationFinalMetadataConvergenceSpec extends Specification {
    ConvergingMetadataAdmin fake=new ConvergingMetadataAdmin()
    def original
    TopicPlan operation
    def setup() {
        fake.state=snapshot(['acl-test':[[0],[0]]],[0:'a',1:'b',2:'c']);original=fake.state
        def service=new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
        def s=service.readReplicationSnapshot(['acl-test'] as Set)
        def rf=new ReplicaAssignmentPlanner().plan(s,['acl-test':2],RACK_AWARE,true,false)['acl-test']
        operation=new TopicPlan.Builder().setName('acl-test').setAction(UPDATE).setReplicationPlan(rf).build()
    }

    def applyWithLag(String condition,int reads) {
        def clock=new AdminReassignmentExecutor.TimeSource() {
            long nanoTime() { System.nanoTime() }
            void pause(long nanos) { java.util.concurrent.TimeUnit.NANOSECONDS.sleep(nanos) }
        }
        BiFunction execution={ engine,request ->
            def result=engine.execute(request)
            assert result.state()==ReassignmentExecutor.State.COMPLETED
            def current=fake.state
            fake.lag=condition=='old assignment' ? original : topic(current,'acl-test',[partitions:current.topics()['acl-test'].partitions().collect { p ->
                new ReplicationClusterSnapshot.Partition(p.id(),condition=='foreign target' ? p.replicas().reverse() : p.replicas(),
                        p.leader(),condition=='retained ISR loss' ? [] as Set : [0] as Set)
            }])
            if(condition=='identity')fake.lag=changed(current,[clusterId:'ERITFBUWFxgZGhscHR4fIA'])
            fake.remaining=reads
            result
        } as BiFunction
        new AdminReplicationApplication({ fake.admin } as Supplier,
                new AdminReassignmentExecutor.Settings(100,Duration.ofMillis(200),Duration.ofMillis(1)),clock,
                { admin,names,timeout -> ReplicationMetadataReader.read(admin,names,timeout,true) } as AdminReassignmentExecutor.SnapshotReader,
                Duration.ofSeconds(3),execution).apply([operation],permissions())
    }

    void 'independent verification waits for cached #condition without sending again'() {
        expect:
        applyWithLag(condition,2).state()==ReassignmentExecutor.State.COMPLETED
        fake.writes==1
        fake.lagReads==2

        where:
        condition << ['old ISR','old assignment']
    }

    void 'independent verification stops immediately on #condition'() {
        when:
        applyWithLag(condition,Integer.MAX_VALUE)

        then:
        def error=thrown(ReplicationExecutionException)
        error.phase==ReplicationExecutionException.Phase.VERIFICATION
        error.result.state()==ReassignmentExecutor.State.UNKNOWN
        error.result.partitions().values().every { it.submission()==ReassignmentExecutor.Submission.SENT && it.lastKnownState()==ReassignmentExecutor.State.COMPLETED }
        fake.writes==1
        fake.lagReads==(condition=='identity' ? 2 : 1) // One observation; identity also changes describeCluster.

        where:
        condition << ['retained ISR loss','foreign target','identity']
    }

    void 'permanently incomplete target ISR never becomes completed and never resubmits'() {
        when:
        applyWithLag('old ISR',Integer.MAX_VALUE)

        then:
        def error=thrown(ReplicationExecutionException)
        error.result.state()==ReassignmentExecutor.State.UNKNOWN
        fake.writes==1
    }
}

class ConvergingMetadataAdmin extends Task2ExecutionAdmin {
    def lag
    int remaining
    int lagReads
    ConvergingMetadataAdmin() {
        brokerEntries=(0..2).collectEntries { [(it):[]] }
        feature=[finalized:['metadata.version':new FinalizedVersionRange((short)19,(short)19)],
                 supported:['metadata.version':new SupportedVersionRange((short)1,(short)19)]]
    }
    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if(remaining>0 && method.name=='describeTopics') {
            remaining--;lagReads++;def actual=state;state=lag
            try { return super.invoke(proxy,method,args) } finally { state=actual }
        }
        if(remaining>0 && lag.clusterId()!=state.clusterId() && method.name=='describeCluster') {
            lagReads++;def actual=state;state=lag
            try { return super.invoke(proxy,method,args) } finally { state=actual }
        }
        super.invoke(proxy,method,args)
    }
}
