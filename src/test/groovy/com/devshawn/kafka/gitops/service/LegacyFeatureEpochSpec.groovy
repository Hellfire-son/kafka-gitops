// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import org.apache.kafka.clients.admin.*
import spock.lang.Specification
import java.lang.reflect.Method
import java.util.function.Supplier

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.snapshot
import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.permissions
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE

class LegacyFeatureEpochSpec extends Specification {
    LegacyEpochAdmin fake=new LegacyEpochAdmin()
    def service() {
        new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
    }

    void 'positive legacy feature bound survives different broker image offsets #epochs'() {
        given:
        fake.epochs=epochs

        when:
        def s=service().readReplicationPreflightSnapshot(['acl-test'] as Set)

        then:
        s.safety().elrMode()==ReplicationSafetyEvidence.ElrMode.NOT_APPLICABLE
        s.safety().featuresStable()==VERIFIED
        s.topics()['acl-test'].partitions().every { it.elr().empty && it.lastKnownElr().empty }
        fake.writes==0

        where:
        epochs << [[200L,199L],[199L,200L]]
    }

    void 'legacy feature bound does not conceal changed #field map'() {
        given:
        fake.changedMap=field

        expect:
        service().readReplicationPreflightSnapshot(['acl-test'] as Set).safety().featuresStable()==UNAVAILABLE
        fake.writes==0

        where:
        field << ['finalized','supported']
    }

    void 'unavailable legacy feature information remains unknown without a mode proof'() {
        given:
        fake.feature=[failure:true]

        when:
        def s=service().readReplicationPreflightSnapshot(['acl-test'] as Set)

        then:
        s.safety().elrMode()==ReplicationSafetyEvidence.ElrMode.UNKNOWN
        s.safety().featuresStable()==UNKNOWN
        fake.writes==0
    }

    void 'legacy RF #previous -> #target completes with real reader and differing broker epochs'() {
        given:
        fake.state=snapshot(['acl-test':previous],[0:'a',1:'b',2:'c'])
        def s=service().readReplicationSnapshot(['acl-test'] as Set)
        def rf=new ReplicaAssignmentPlanner().plan(s,['acl-test':target],RACK_AWARE,true,false)['acl-test']
        def operation=new TopicPlan.Builder().setName('acl-test').setAction(UPDATE).setReplicationPlan(rf).build()
        fake.epochCalls=0
        def app=new AdminReplicationApplication({ fake.admin } as Supplier)

        expect:
        app.apply([operation],permissions()).state()==ReassignmentExecutor.State.COMPLETED
        fake.writes==1

        where:
        previous | target
        [[0],[1]] | 2
        [[0,1],[1,2]] | 3
        [[0,1,2],[1,2,0]] | 2
    }
}

class LegacyEpochAdmin extends Task2ExecutionAdmin {
    List<Long> epochs=[200L,199L]
    int epochCalls
    String changedMap
    LegacyEpochAdmin() {
        brokerEntries=(0..2).collectEntries { [(it):[]] }
        feature=[finalized:['metadata.version':new FinalizedVersionRange((short)19,(short)19)],
                 supported:['metadata.version':new SupportedVersionRange((short)1,(short)19)]]
    }
    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if(method.name=='describeFeatures') {
            feature.epoch=Optional.of(epochs[epochCalls++ % epochs.size()])
            if(changedMap && epochCalls==2) {
                feature[changedMap]=changedMap=='finalized'
                        ? ['metadata.version':new FinalizedVersionRange((short)18,(short)18)]
                        : ['metadata.version':new SupportedVersionRange((short)1,(short)18)]
            }
        }
        super.invoke(proxy,method,args)
    }
}
