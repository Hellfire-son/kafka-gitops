package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.Admission
import com.devshawn.kafka.gitops.service.ReplicaAssignmentPlanner
import com.devshawn.kafka.gitops.service.ReplicationSafetyPolicy

import java.time.Duration

import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.DISABLED
import static com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE

class ReplicationSafetyTestSupport {
    static Map fixture(List<List<Integer>> previous = [[0],[1]], int target = 2) {
        def base = snapshot([orders:previous])
        [base:base, plan:new ReplicaAssignmentPlanner().plan(base,[orders:target],RACK_AWARE,true,false).orders]
    }

    /** Test-only attestations; real 3.9 reads never manufacture VERIFIED placement or disk reserve. */
    static ReplicationClusterSnapshot observed(ReplicationClusterSnapshot s, Map overrides = [:]) {
        def evidence = new ReplicationSafetyEvidence(overrides.getOrDefault('completedAtNanos',OptionalLong.of(System.nanoTime())),
                overrides.getOrDefault('elrMode',DISABLED),overrides.getOrDefault('featuresStable',VERIFIED),
                overrides.getOrDefault('configsStable',s.topics().keySet().collectEntries { [(it):VERIFIED] }),
                overrides.getOrDefault('cordoningStable',s.brokers().keySet().collectEntries { [(it):VERIFIED] }),
                overrides.getOrDefault('admissions',s.brokers().keySet().collectEntries { [(it):new Admission(VERIFIED,VERIFIED)] }))
        new ReplicationClusterSnapshot(s.clusterId(),s.brokers(),s.topics(),s.reassignments(),s.reassignmentState(),
                s.replicaCounts(),s.loadState(),evidence)
    }

    static ReplicationSafetyPolicy.Constraints constraints(boolean config = false, boolean partition = false) {
        long now = System.nanoTime()
        new ReplicationSafetyPolicy.Constraints(partition,config,now - 1000000000L,now,Duration.ofSeconds(30))
    }

    static ReplicationSafetyPolicy.Permissions permissions(boolean manage = true, boolean decrease = true, boolean noDelete = false) {
        new ReplicationSafetyPolicy.Permissions(manage,decrease,noDelete)
    }
}
