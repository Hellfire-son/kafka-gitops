"""Offline safety regression for the integration runner. No filesystem/network/Docker/Kafka operations."""
import sys
import unittest
sys.dont_write_bytecode=True
from kafka44_lab import LabRun, CLUSTER

class PlanScopeTest(unittest.TestCase):
    def setUp(self):
        self.run=LabRun.__new__(LabRun)
        self.run.prefix='gitops-it-offline-'
        self.run.manifest={'topics':{'main':'gitops-it-offline-main'}}
        self.plan={'topicPlans':[{'name':'gitops-it-offline-main','action':'UPDATE',
                    'topicConfigPlans':[{'key':'max.message.bytes','action':'UPDATE','value':'1048588'}]}],'aclPlans':[]}

    def test_scoped_config_is_allowed(self):
        self.run.check_plan(self.plan)

    def test_foreign_topic_is_refused(self):
        self.plan['topicPlans'][0]['name']='foreign-topic'
        with self.assertRaises(AssertionError):self.run.check_plan(self.plan)

    def test_topic_delete_is_refused_even_inside_own_namespace(self):
        self.plan['topicPlans'][0]['action']='REMOVE'
        with self.assertRaises(AssertionError):self.run.check_plan(self.plan)

    def test_acl_operations_are_refused(self):
        self.plan['aclPlans']=[{'action':'ADD'}]
        with self.assertRaises(AssertionError):self.run.check_plan(self.plan)

    def test_unknown_configuration_is_refused(self):
        self.plan['topicPlans'][0]['topicConfigPlans'][0]['key']='sasl.jaas.config'
        with self.assertRaises(AssertionError):self.run.check_plan(self.plan)

    def test_wrong_rf_cluster_identity_is_refused(self):
        self.plan['topicPlans'][0]['replicationPlan']={'clusterId':'WRONG_CLUSTER'}
        with self.assertRaises(AssertionError):self.run.check_plan(self.plan)

if __name__=='__main__':unittest.main()
