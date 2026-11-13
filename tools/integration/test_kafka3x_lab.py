# SPDX-License-Identifier: Apache-2.0
"""Offline tests for separate legacy gates. No Docker/Kafka/filesystem mutations."""
import copy, pathlib, subprocess, sys, unittest
from unittest.mock import Mock, patch
sys.dont_write_bytecode=True
import kafka3x_lab as lab

class LegacyScopeTest(unittest.TestCase):
    def setUp(self):
        lab.select_profile('371');self.run=lab.LegacyRun.__new__(lab.LegacyRun)
        self.run.prefix='gitops-it-offline-';self.name=self.run.prefix+'rf';self.run.manifest={'topics':{'rf':self.name}}
        self.before={'topics':{self.name:{'topicId':'uuid','configs':{},'partitions':[
            dict(id=i,replicas=[1,2,3],isr=[1,2,3],leader=1,elr='UNKNOWN',lastKnownElr='UNKNOWN') for i in range(2)]}}}
        self.plan={'aclPlans':[],'topicPlans':[dict(name=self.name,action='UPDATE',replicationPlan=dict(
            clusterId=lab.CLUSTER,topicId='uuid',schemaVersion=1,partitionCount=2,targetReplicationFactor=2,
            expectedMinInSyncReplicas=1,brokerSnapshot=[{'id':i} for i in (1,2,3)],assignments=[
                dict(partition=i,previousReplicas=[1,2,3],targetReplicas=[1,2]) for i in range(2)]))]}

    def test_legacy_absent_elr_is_not_replaced_with_empty(self):
        self.run.check_rf_plan(self.plan,self.before,2)
        self.assertEqual(self.before['topics'][self.name]['partitions'][0]['elr'],'UNKNOWN')

    def test_scope_rejects_foreign_resources_actions_acls_and_configs(self):
        changes=[lambda p:p.update(aclPlans=[{'action':'ADD'}]),
                 lambda p:p['topicPlans'][0].update(name='foreign'),lambda p:p['topicPlans'][0].update(action='REMOVE'),
                 lambda p:p['topicPlans'][0].update(action='UNKNOWN'),
                 lambda p:p['topicPlans'][0].update(topicConfigPlans=[{'key':'sasl.jaas.config'}]),
                 lambda p:p['topicPlans'][0].update(topicDetails={'configs':{'ssl.key.password':'secret'}}),
                 lambda p:p['topicPlans'][0].update(partitionPlan={'action':'UPDATE'})]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                p=copy.deepcopy(self.plan);change(p)
                with self.assertRaises(AssertionError):self.run.check_rf_plan(p,self.before,2)

    def test_rf_identity_assignments_and_leader_are_required(self):
        changes=[lambda r:r.update(clusterId='4L6g3nShT-eMCtK--X86sw'),lambda r:r.update(topicId='foreign'),
                 lambda r:r.update(brokerSnapshot=[{'id':4}]),lambda r:r.update(partitionCount=3),
                 lambda r:r['assignments'][0].update(previousReplicas=[3,2,1]),
                 lambda r:r['assignments'][0].update(targetReplicas=[2,3]),
                 lambda r:r['assignments'][0].update(targetReplicas=[1,9]),
                 lambda r:r['assignments'][0].update(targetReplicas=[1,1])]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                p=copy.deepcopy(self.plan);change(p['topicPlans'][0]['replicationPlan'])
                with self.assertRaises(AssertionError):self.run.check_rf_plan(p,self.before,2)

    def test_incomplete_isr_blocks_rf_scope(self):
        self.before['topics'][self.name]['partitions'][0]['isr']=[1]
        with self.assertRaises(AssertionError):self.run.check_rf_plan(self.plan,self.before,2)

    def test_profiles_have_separate_ports_projects_and_directories(self):
        first=(lab.BOOTSTRAP,lab.PROJECT,lab.LAB)
        lab.select_profile('323');second=(lab.BOOTSTRAP,lab.PROJECT,lab.LAB)
        self.assertTrue(all(a!=b for a,b in zip(first,second)))
        self.assertNotIn('19092',lab.BOOTSTRAP)
        with self.assertRaises(AssertionError):lab.select_profile('440')

    def test_write_phases_require_opt_in_before_lab_access(self):
        script=pathlib.Path(__file__).with_name('kafka3x_lab.py')
        for phase in ['basic','rf','negative']:
            r=subprocess.run([sys.executable,'-B',str(script),'--version','371','--phase',phase],capture_output=True,text=True)
            self.assertEqual(r.returncode,2);self.assertIn('Explicit --allow-writes',r.stderr)

    def test_exact_desktop_mirror_and_windows_config_bind_are_allowed(self):
        lab.select_profile('323');name='broker-1.properties'
        for source in lab.bind_source_paths(str((lab.LAB/'config'/name).resolve())):
            lab.verify_config_bind(dict(Source=source,Destination='/opt/lab/config/'+name,RW=False),name)

    def test_foreign_writable_or_misdirected_config_mount_is_refused(self):
        lab.select_profile('323');name='broker-1.properties'
        binding=dict(Source=str((lab.LAB/'config'/name).resolve()),Destination='/opt/lab/config/'+name,RW=False)
        for change in [dict(Source='/different-lab/config/'+name),dict(RW=True),dict(Destination='/tmp/config')]:
            with self.assertRaises(AssertionError):lab.verify_config_bind(dict(binding,**change),name)
        with self.assertRaises(AssertionError):lab.verify_config_bind(binding,'../foreign.properties')

    def test_changed_jar_blocks_child_command_before_any_io(self):
        self.run.manifest['jarSha256']=lab.hashlib.sha256(b'original').hexdigest()
        with patch.object(lab,'JAR',Mock(read_bytes=lambda:b'changed')),patch.object(lab,'capture') as child:
            with self.assertRaises(AssertionError):self.run.command('write',[])
            child.assert_not_called()

if __name__=='__main__':unittest.main()
