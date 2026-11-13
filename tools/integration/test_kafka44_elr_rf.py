"""Offline guard checks. No Docker, network, Kafka, or lab writes."""
import copy, sys, unittest
sys.dont_write_bytecode=True
from kafka44_elr_rf import ElrRun, CLUSTER

class RfScopeTest(unittest.TestCase):
    def setUp(self):
        self.run=ElrRun.__new__(ElrRun);self.run.prefix='gitops-it-offline-'
        self.name=self.run.prefix+'rf';self.run.manifest={'topics':{'rf':self.name}}
        parts=[dict(id=i,leader=1,replicas=[1,2,3],isr=[1,2,3],elr=[],lastKnownElr=[]) for i in range(2)]
        self.before={'topics':{self.name:{'topicId':'uuid','partitions':parts,'configs':{'max.message.bytes':{'value':'1048588'}}}}}
        self.plan={'aclPlans':[],'topicPlans':[{'name':self.name,'action':'UPDATE','replicationPlan':{
            'schemaVersion':1,'clusterId':CLUSTER,'topicId':'uuid','partitionCount':2,'targetReplicationFactor':2,
            'expectedMinInSyncReplicas':1,'brokerSnapshot':[{'id':i} for i in (1,2,3)],
            'assignments':[{'partition':i,'previousReplicas':[1,2,3],'targetReplicas':[1,2]} for i in range(2)]}}]}

    def test_only_exact_rf_decrease_is_allowed(self):
        self.run.check_rf_plan(self.plan,self.before,2)

    def test_unchanged_desired_state_context_is_allowed(self):
        self.plan['topicPlans'][0]['topicDetails']={'partitions':2,'replication':2,'configs':{'max.message.bytes':'1048588'}}
        self.run.check_rf_plan(self.plan,self.before,2)

    def test_scope_violations_are_refused(self):
        mutations=[
            lambda p:p['aclPlans'].append({'action':'ADD'}),
            lambda p:p['topicPlans'].append(copy.deepcopy(p['topicPlans'][0])),
            lambda p:p['topicPlans'][0].update(name='foreign'),
            lambda p:p['topicPlans'][0].update(action='REMOVE'),
            lambda p:p['topicPlans'][0].update(partitionPlan={'action':'UPDATE'}),
            lambda p:p['topicPlans'][0].update(topicConfigPlans=[{'key':'retention.ms'}]),
            lambda p:p['topicPlans'][0].update(topicDetails={'partitions':2}),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutations.index(mutate)):
                p=copy.deepcopy(self.plan);mutate(p)
                with self.assertRaises(AssertionError):self.run.check_rf_plan(p,self.before,2)

    def test_identity_and_assignment_violations_are_refused(self):
        mutations=[
            lambda r:r.update(clusterId='wrong'),lambda r:r.update(topicId='wrong'),
            lambda r:r.update(schemaVersion=2),lambda r:r.update(partitionCount=3),
            lambda r:r.update(targetReplicationFactor=1),lambda r:r.update(expectedMinInSyncReplicas=3),
            lambda r:r.update(brokerSnapshot=[{'id':9}]),
            lambda r:r['assignments'][0].update(partition=1),
            lambda r:r['assignments'][0].update(previousReplicas=[3,2,1]),
            lambda r:r['assignments'][0].update(targetReplicas=[2,3]),
            lambda r:r['assignments'][0].update(targetReplicas=[1,9]),
            lambda r:r['assignments'][0].update(targetReplicas=[1,1]),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutations.index(mutate)):
                p=copy.deepcopy(self.plan);mutate(p['topicPlans'][0]['replicationPlan'])
                with self.assertRaises(AssertionError):self.run.check_rf_plan(p,self.before,2)

    def test_unhealthy_or_unknown_source_is_refused(self):
        for change in [dict(isr=[1,2]),dict(elr='UNKNOWN'),dict(lastKnownElr='UNKNOWN'),dict(elr=[3]),dict(lastKnownElr=[3])]:
            with self.subTest(change=change):
                before=copy.deepcopy(self.before);before['topics'][self.name]['partitions'][0].update(change)
                with self.assertRaises(AssertionError):self.run.check_rf_plan(self.plan,before,2)

    def test_increase_cannot_reorder_current_replicas(self):
        before=copy.deepcopy(self.before)
        for part in before['topics'][self.name]['partitions']:part.update(replicas=[1],isr=[1])
        p=copy.deepcopy(self.plan)
        for a in p['topicPlans'][0]['replicationPlan']['assignments']:a.update(previousReplicas=[1],targetReplicas=[2,1])
        with self.assertRaises(AssertionError):self.run.check_rf_plan(p,before,2)

if __name__=='__main__':unittest.main()
