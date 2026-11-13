package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.*
import spock.lang.Specification

import java.lang.reflect.Method
import java.time.Duration
import java.util.function.BiFunction
import java.util.function.Supplier

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.permissions
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE

class ElrApplicationSpec extends Specification {
    EnabledElrAdmin fake
    KafkaService service
    def setup() { setupCluster([[0],[0]],2) }

    void setupCluster(List<List<Integer>> replicas,int target) {
        fake=new EnabledElrAdmin()
        fake.state=snapshot(['acl-test':replicas],[0:'a',1:'b',2:'c'])
        service=new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
    }
    TopicPlan plan(int target) {
        def snapshot=service.readReplicationSnapshot(['acl-test'] as Set)
        def rf=new ReplicaAssignmentPlanner().plan(snapshot,['acl-test':target],RACK_AWARE,true,false)['acl-test']
        new TopicPlan.Builder().setName('acl-test').setAction(UPDATE).setReplicationPlan(rf).build()
    }
    ReplicationApplication app(BiFunction execution=null,int batch=100) {
        def time=new AdminReassignmentExecutor.TimeSource() {
            long nanoTime() { System.nanoTime() }
            void pause(long nanos) { java.util.concurrent.TimeUnit.NANOSECONDS.sleep(nanos) }
        }
        new AdminReplicationApplication({ fake.admin } as Supplier,
                new AdminReassignmentExecutor.Settings(batch,Duration.ofSeconds(1),Duration.ofMillis(1)),time,
                { admin,names,timeout -> ReplicationMetadataReader.read(admin,names,timeout,true) } as AdminReassignmentExecutor.SnapshotReader,
                Duration.ofSeconds(3), execution ?: ({ engine,request -> engine.execute(request) } as BiFunction))
    }

    void 'enabled ELR real reader policy executor and verification complete exact #previous -> #target'() {
        given:
        setupCluster(previous,target)
        def operation=plan(target)

        when:
        def result=app().apply([operation],permissions())

        then:
        result.state()==ReassignmentExecutor.State.COMPLETED
        fake.writes==1
        fake.calls.count('close')==2
        fake.state.topics()['acl-test'].partitions()*.replicas()==operation.replicationPlan.get().assignments*.targetReplicas
        fake.calls.count('describeFeatures')>=7 // Fresh preflight, per-batch, terminal and independent verification.

        where:
        previous | target
        [[0],[0]] | 2
        [[0,1],[1,0]] | 3
        [[0,1,2],[1,2,0]] | 2
    }

    void 'enabled ELR safety failure #condition never reaches any write'() {
        given:
        def operation=plan(2)
        switch(condition) {
            case 'ELR nonempty':fake.elr=[0];break
            case 'last known nonempty':fake.last=[0];break
            case 'fallback unavailable':fake.fallback=true;break
            case 'feature unknown':fake.feature=[failure:true];break
            case 'cluster ID':fake.state=changed(fake.state,[clusterId:'ERITFBUWFxgZGhscHR4fIA']);break
            case 'topic UUID':fake.state=topic(fake.state,'acl-test',[topicId:'ERITFBUWFxgZGhscHR4fIA']);break
            case 'ISR loss':
                def t=fake.state.topics()['acl-test'];fake.state=topic(fake.state,'acl-test',[partitions:t.partitions().collect { new ReplicationClusterSnapshot.Partition(it.id(),it.replicas(),it.leader(),[] as Set) }]);break
            case 'leader lost':
                def t=fake.state.topics()['acl-test'];fake.state=topic(fake.state,'acl-test',[partitions:t.partitions().collect { new ReplicationClusterSnapshot.Partition(it.id(),it.replicas(),OptionalInt.empty(),it.isr()) }]);break
            case 'min ISR changed':fake.topicEntries=[ReplicationMetadataSpec.entry('min.insync.replicas','2')];break
            case 'target below min ISR':fake.topicEntries=[ReplicationMetadataSpec.entry('min.insync.replicas','3')];break
            case 'conflict':fake.active=[(new TopicPartition('acl-test',0)):new PartitionReassignment([0,2],[2],[])];break
        }

        when:
        app().apply([operation],permissions())

        then:
        def error=thrown(ReplicationPreflightException)
        error.message.contains(reason)
        !error.message.contains('private-')
        fake.writes==0

        where:
        condition | reason
        'ELR nonempty' | 'ELR_PARTITION_UNSAFE'
        'last known nonempty' | 'ELR_PARTITION_UNSAFE'
        'fallback unavailable' | 'ELR_UNSUPPORTED'
        'feature unknown' | 'ELR_MODE_UNKNOWN'
        'cluster ID' | 'CLUSTER_ID_CHANGED'
        'topic UUID' | 'TOPIC_ID_CHANGED'
        'ISR loss' | 'ISR_INCOMPLETE'
        'leader lost' | 'LEADER_UNKNOWN'
        'min ISR changed' | 'MIN_ISR_CHANGED'
        'target below min ISR' | 'TARGET_BELOW_MIN_ISR'
        'conflict' | 'ACTIVE_REASSIGNMENT'
    }

    void 'decrease permission is independent of ELR and API options: #allow / #noDelete'() {
        given:
        setupCluster([[0,1,2]],2)
        def operation=plan(2)
        int reads=fake.calls.size()

        when:
        app().apply([operation],permissions(true,allow,noDelete))

        then:
        def error=thrown(ReplicationPreflightException)
        error.message.contains(reason)
        fake.writes==0
        fake.calls.size()==reads

        where:
        allow | noDelete | reason
        false | false | 'DECREASE_PERMISSION_REQUIRED'
        true | true | 'NO_DELETE'
    }

    void 'dangerous #condition after first packet stops every subsequent packet'() {
        given:
        def operation=plan(2)
        fake.afterAlter={
            if(condition=='ELR') fake.elr=[0]
            else if(condition=='LastKnownELR') fake.last=[0]
            else {
                def t=fake.state.topics()['acl-test']
                fake.state=topic(fake.state,'acl-test',[partitions:t.partitions().collect {
                    if(it.id()!=0) return it
                    new ReplicationClusterSnapshot.Partition(it.id(),it.replicas(),
                            condition=='leader' ? OptionalInt.empty() : it.leader(),condition=='ISR' ? [1] as Set : it.isr())
                }])
            }
        }

        when:
        app(null,1).apply([operation],permissions())

        then:
        def error=thrown(ReplicationExecutionException)
        error.result.state()!=ReassignmentExecutor.State.COMPLETED
        fake.writes==1
        error.message.contains('ELR_UNSAFE')

        where:
        condition << ['ELR','LastKnownELR','ISR','leader']
    }

    void 'terminal safety rereads #condition and refuses completion'() {
        given:
        def operation=plan(2)
        fake.afterAlter={
            if(condition=='min ISR') fake.topicEntries=[ReplicationMetadataSpec.entry('min.insync.replicas','2')]
            else fake.feature=[finalized:['eligible.leader.replicas.version':new FinalizedVersionRange((short)0,(short)0)]]
        }

        when:
        app().apply([operation],permissions())

        then:
        def error=thrown(ReplicationExecutionException)
        error.result.state()!=ReassignmentExecutor.State.COMPLETED
        fake.writes==1

        where:
        condition << ['min ISR','feature mode']
    }

    void 'independent final verification catches ELR danger after executor completed'() {
        given:
        def operation=plan(2)
        BiFunction execution={ engine,request ->
            def result=engine.execute(request)
            assert result.state()==ReassignmentExecutor.State.COMPLETED
            fake.last=[0]
            result
        } as BiFunction

        when:
        app(execution).apply([operation],permissions())

        then:
        def error=thrown(ReplicationExecutionException)
        error.phase==ReplicationExecutionException.Phase.VERIFICATION
        error.result.partitions().values().every { it.submission()==ReassignmentExecutor.Submission.SENT
                && it.lastKnownState()==ReassignmentExecutor.State.COMPLETED && it.state()==ReassignmentExecutor.State.UNKNOWN }
        fake.writes==1
    }

    void 'safe rerun observes target and preserves saved assignments without submitting twice'() {
        given:
        def operation=plan(2)
        def serialized=com.devshawn.kafka.gitops.PlanTestSupport.mapper().writeValueAsString(operation)
        assert app().apply([operation],permissions()).state()==ReassignmentExecutor.State.COMPLETED

        expect:
        app().apply([operation],permissions()).state()==ReassignmentExecutor.State.COMPLETED
        fake.writes==1
        com.devshawn.kafka.gitops.PlanTestSupport.mapper().writeValueAsString(operation)==serialized
    }

    void 'enabled ELR permits advancing metadata offsets with identical feature maps throughout apply'() {
        given:
        fake.epochStep=1
        def operation=plan(2)

        expect:
        app().apply([operation],permissions()).state()==ReassignmentExecutor.State.COMPLETED
        fake.writes==1
    }

    void 'enabled ELR refuses #change feature evidence before writes'() {
        given:
        def operation=plan(2)
        if(change=='regressing epoch') fake.epochStep=-1
        else if(change=='negative epoch') fake.feature.epoch=Optional.of(-1L)
        else if(change=='missing epoch') fake.feature.epoch=Optional.empty()
        else fake.changeFeatureMap=true

        when:
        app().apply([operation],permissions())

        then:
        thrown(ReplicationPreflightException)
        fake.writes==0

        where:
        change << ['regressing epoch','negative epoch','missing epoch','changed feature map']
    }
}

class EnabledElrAdmin extends Task2ExecutionAdmin {
    List<Integer> elr=[]
    List<Integer> last=[]
    boolean fallback
    Closure afterAlter
    int epochStep
    long nextEpoch=100
    boolean changeFeatureMap
    EnabledElrAdmin() {
        feature=[finalized:['eligible.leader.replicas.version':new FinalizedVersionRange((short)1,(short)1),
                           'metadata.version':new FinalizedVersionRange((short)33,(short)33)]]
        brokerEntries=(0..2).collectEntries { [(it):[ReplicationMetadataSpec.entry('cordoned.log.dirs','')]] }
    }
    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if(method.name=='describeFeatures') {
            if(epochStep) { feature.epoch=Optional.of(nextEpoch);nextEpoch+=epochStep }
            if(changeFeatureMap) {
                feature.finalized=new HashMap(feature.finalized)
                feature.finalized['metadata.version']=new FinalizedVersionRange((short)nextEpoch,(short)nextEpoch)
                nextEpoch++
            }
        }
        if(method.name=='describeTopics') {
            def original=super.invoke(proxy,method,args) as DescribeTopicsResult
            if(fallback) return original
            def descriptions=original.allTopicNames().get(1,java.util.concurrent.TimeUnit.SECONDS).collectEntries { name,t ->
                def node={ id -> new Node(id,'unused.invalid',1,state.brokers()[id]?.rack()?.orElse(null)) }
                [(name):KafkaFuture.completedFuture(new TopicDescription(name,false,t.partitions().collect { p ->
                    new TopicPartitionInfo(p.partition(),p.leader(),p.replicas(),p.isr(),elr.collect(node),last.collect(node))
                },[] as Set,t.topicId()))]
            }
            return new DescribeTopicsResult(null,descriptions)
        }
        def value=super.invoke(proxy,method,args)
        if(method.name=='alterPartitionReassignments') afterAlter?.call()
        value
    }
}
