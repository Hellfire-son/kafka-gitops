"""Opt-in ELR-enabled RF tests. Reuses the original Docker/identity gate; never cleans up resources."""
import argparse, hashlib, json, os, pathlib, subprocess, time
from kafka44_lab import LabRun, ROOT, JAR, JAVA, PROPS, CLUSTER, ENV, read, save

IMAGE='sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f'

class ElrRun(LabRun):
    def __init__(self,run_dir=None):
        if run_dir:assert read(pathlib.Path(run_dir)/'manifest.json').get('kind')=='elr-rf','Refusing a previous integration run'
        super().__init__(run_dir)
        self.manifest['kind']='elr-rf';self.persist()

    def persist(self):
        self.manifest['jarSha256']=hashlib.sha256(JAR.read_bytes()).hexdigest()
        save(self.folder/'manifest.json',self.manifest)
        save(ROOT/'latest-elr-run.json',dict(runDir=str(self.folder),prefix=self.manifest['prefix']))

    def probe(self,kind,output,*extra):
        return [JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),
                'com.devshawn.kafka.gitops.KafkaElrProbe',kind,PROPS,self.prefix,self.manifest['topics']['rf'],output,*extra]

    def gate(self,id):
        observed=super().gate(id)
        assert observed['finalizedFeatures']['eligible.leader.replicas.version']=={'min':1,'max':1}
        proof=read(self.folder/(id+'-docker-proof.json'))
        assert all(c['image']==IMAGE for c in proof['containers']),'Unexpected runtime image digest'
        expected=self.manifest.get('composeSha256')
        assert expected is None or expected==proof['composeSha256'],'Compose changed during run'
        self.manifest['composeSha256']=proof['composeSha256']
        assert not observed['reassignments'],'No competing transfer allowed in this isolated run'
        assert all(c['min.insync.replicas']=='1' and c['cordoned.log.dirs']=='' for c in observed['brokerConfigs'].values())
        return observed

    def prior_snapshot(self,id):
        output=self.folder/(id+'-prior-topics.json')
        args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),
              'com.devshawn.kafka.gitops.KafkaLabProbe','snapshot',PROPS,'gitops-it-',output]
        self.command(id+'-prior-snapshot',args,0)
        return {n:t for n,t in read(output)['topics'].items() if not n.startswith(self.prefix)}

    def readonly(self):
        super().readonly()
        prior=self.prior_snapshot('E00');save(self.folder/'prior-baseline.json',prior)
        self.record('E00','PASS','Preserve prior test topics/evidence','Prior topic UUID/config/assignment baseline saved; separate latest-elr-run pointer',exit_code=0)

    def check_rf_plan(self,plan,before,target):
        self.check_plan(plan)
        assert not plan.get('aclPlans') and len(plan['topicPlans'])==1
        t=plan['topicPlans'][0]
        assert t['name']==self.manifest['topics']['rf'] and t['action']=='UPDATE'
        assert not t.get('partitionPlan') and not t.get('topicConfigPlans')
        rp=t['replicationPlan'];actual=before['topics'][t['name']]
        # topicDetails is desired-state context in the established JSON schema, not a write operation.
        details=t.get('topicDetails')
        if details:
            assert details.get('partitions')==len(actual['partitions']) and details.get('replication')==target
            assert all(k in actual['configs'] and str(v)==actual['configs'][k]['value'] for k,v in details.get('configs',{}).items())
        assert rp['clusterId']==CLUSTER and rp['topicId']==actual['topicId']
        assert rp['schemaVersion']==1 and rp['partitionCount']==len(actual['partitions'])==2
        assert rp['targetReplicationFactor']==target and rp['expectedMinInSyncReplicas']==1 and target>=1
        assert {b['id'] for b in rp['brokerSnapshot']}=={1,2,3}
        assert len(rp['assignments'])==2 and {a['partition'] for a in rp['assignments']}=={0,1}
        for a in rp['assignments']:
            p=next(p for p in actual['partitions'] if p['id']==a['partition'])
            assert a['previousReplicas']==p['replicas'] and len(a['targetReplicas'])==target
            assert len(set(a['targetReplicas']))==target and set(a['targetReplicas'])<={1,2,3}
            assert p['leader'] in a['targetReplicas']
            assert set(p['replicas'])==set(p['isr']) and p['elr']==p['lastKnownElr']==[]
            if target>len(p['replicas']):assert a['targetReplicas'][:len(p['replicas'])]==p['replicas']
            else:assert set(a['targetReplicas'])<=set(p['isr'])
        return rp

    def data_check(self,id):
        output=self.folder/(id+'-data.json');args=self.probe('check-data',output,self.folder/'seed-records.json')
        self.command(id+'-data',args,0);assert read(output)['matched']==12
        self.record(id+'-DATA','PASS','Preserve all synthetic records and offsets','12/12 keys, values, partitions and offsets unchanged; no consumer commits',args,0)

    def guarded_apply(self,id,yaml,plan,expected,no_delete=True,extra=()):
        self.gate(id+'-before')
        args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.KafkaLabProbe',
              'plan',PROPS,self.folder/(id+'-api-audit.json')]+self.args(yaml,'apply',no_delete,['--plan',plan,*extra])[3:]
        result=self.command(id,args,expected)
        assert read(self.folder/(id+'-api-audit.json'))['writeAttempts']==0
        return result,args

    def transfer(self,id,target,decrease=False):
        before=self.gate(id+'-plan-before');y=self.yaml(id,'rf',rf=target)
        flags=['--allow-replication-decrease'] if decrease else []
        try:p,planned,_=self.plan(id+'-plan',y,no_delete=not decrease,guarded=True,extra=flags)
        except AssertionError:
            self.record(id,'BLOCKED','Executable RF plan with verified ELR','Planning refused; inspect '+id+'-plan.log')
            raise
        rp=self.check_rf_plan(read(p),before,target)
        assert 'ELR_VERIFIED_EMPTY' in planned.stdout and 'ELR feature: ENABLED' in planned.stdout
        if decrease:
            for label,no_delete,opts in [('N02',False,[]),('N03',True,flags)]:
                result,args=self.guarded_apply(label,y,p,2,no_delete,opts)
                assert self.snapshot(label+'-after')['topics']==before['topics']
                self.record(label,'PASS','Reject unauthorized decrease without write attempts','Exit2; zero Admin write attempts; metadata unchanged',args,2)
        # The full gate is repeated inside apply immediately before the real CLI invocation.
        ready=self.folder/(id+'-watch.ready');stop=self.folder/(id+'-watch.stop');trace=self.folder/(id+'-trace.jsonl')
        with (self.folder/(id+'-watch.log')).open('w',encoding='utf-8') as log:
            watcher=subprocess.Popen([str(a) for a in self.probe('watch',trace,ready,stop)],env=ENV,stdout=log,stderr=log,
                                     creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            try:
                deadline=time.monotonic()+10
                while not ready.exists() and watcher.poll() is None and time.monotonic()<deadline:time.sleep(.1)
                assert ready.exists(),'Observer did not become ready'
                b,after,result,args=self.apply(id+'-apply',y,p,no_delete=not decrease,extra=flags)
            finally:
                stop.write_text('stop',encoding='utf-8');watcher.wait(timeout=12)
        assert watcher.returncode==0
        actual=after['topics'][self.manifest['topics']['rf']]
        assert actual['topicId']==rp['topicId'] and not after['reassignments']
        for a in rp['assignments']:
            part=next(v for v in actual['partitions'] if v['id']==a['partition'])
            assert part['replicas']==a['targetReplicas'] and set(part['isr'])==set(part['replicas'])
            assert part['leader'] in part['isr'] and part['elr']==part['lastKnownElr']==[]
        assert 'COMPLETED' in result.stdout
        observed=[json.loads(line) for line in trace.read_text().splitlines()]
        active=any(s['active'] for s in observed)
        self.independent(id,self.manifest['topics']['rf'])
        self.record(id,'PASS','RF target '+str(target)+' reaches actual completion','Exact ordered target; full ISR; valid leader; ELR/LastKnownELR empty; no active reassignment',args,0,b,after,
                    comment=f'{len(observed)} read-only trace samples; intermediate RUNNING '+('observed' if active else 'not observed (short transfer)'))
        self.record(id+'-RUNNING','PASS' if active else 'NOT_TESTED','Observe intermediate RUNNING independently',
                    'Active reassignment sampled' if active else 'No active sample during short transfer; ACK/completion separation covered by isolated executor tests')
        self.data_check(id)
        _,again,cmd=self.plan(id+'-repeat-plan',y,guarded=True)
        assert 'NO_CHANGES' in again.stdout
        self.record(id+'-REPLAN','PASS','Repeat plan is NO_CHANGES','NO_CHANGES; zero Admin write attempts',cmd,0)
        again,cmd=self.guarded_apply(id+'-repeat-apply',y,p,0,not decrease,flags)
        assert self.snapshot(id+'-repeat-after')['topics']==after['topics']
        self.record(id+'-REAPPLY','PASS','Saved achieved RF plan does not send another write','Exit0; zero Admin write attempts; terminal state independently checked',cmd,0)
        return y,p,after

    def rf(self,resume=False):
        self.manifest.setdefault('writeJarSha256',hashlib.sha256(JAR.read_bytes()).hexdigest());self.persist()
        if resume:
            assert self.manifest['createdTopics']==[self.manifest['topics']['rf']]
            assert all(any(r['id']==id and r['status']=='PASS' for r in self.manifest['results']) for id in ['E01','E02','E02-DATA'])
            current=self.gate('RESUME')
            actual=current['topics'][self.manifest['topics']['rf']]
            assert actual['topicId']==read(self.folder/'seed-records.json')['topicId']
            assert all(len(p['replicas'])==1 and p['elr']==p['lastKnownElr']==[] for p in actual['partitions'])
            assert not any(r['id'] in ['RF12','RF23','RF32'] for r in self.manifest['results'])
            self.data_check('RESUME')
        else:
            assert not self.manifest['createdTopics'],'Use explicit --resume only after inspecting the partial RF1 setup'
            y,p,initial=self.change('E01','rf')
            assert all(p['elr']==p['lastKnownElr']==[] for p in initial['topics'][self.manifest['topics']['rf']]['partitions'])
            self.gate('E02-before')
            args=self.probe('seed',self.folder/'seed-records.json','--allow-writes');self.command('E02-seed',args,0)
            self.record('E02','PASS','Seed only the exact fresh run RF topic','12 synthetic records acknowledged with acks=all/idempotence',args,0)
            self.data_check('E02')
        _,old_plan,_=self.transfer('RF12',2)
        _,_,at_three=self.transfer('RF23',3)
        y=self.yaml('N01','rf',rf=4);_,r,args=self.plan('N01-plan',y,expected=2,guarded=True)
        assert 'INSUFFICIENT_BROKERS' in r.stdout
        self.record('N01','PASS','RF4 rejected on three brokers','INSUFFICIENT_BROKERS; no JSON export; zero Admin write attempts',args,2)
        old_y=self.folder/'RF12.yaml'
        r,args=self.guarded_apply('N04',old_y,old_plan,2)
        assert self.snapshot('N04-after')['topics']==at_three['topics']
        self.record('N04','PASS','Reject saved previous RF1/targetRF2 after actual RF3','Stale assignments rejected; exit2; zero Admin write attempts',args,2)
        self.transfer('RF32',2,True)
        self.verify()

    def verify(self):
        final=self.gate('FINAL')
        assert set(final['allTopics'])==set(self.manifest['baselineTopics'])|set(self.manifest['createdTopics'])
        assert self.prior_snapshot('FINAL')==read(self.folder/'prior-baseline.json'),'Prior test topic changed'
        self.record('FINAL','PASS','Leave prior topics and lab intact','Three containers running; ELR level1; prior UUID/config/assignments unchanged; new test topic and evidence retained',exit_code=0,after=final)

    def audit_final(self):
        assert any(r['id']=='RF32' and r['status']=='PASS' for r in self.manifest['results'])
        before=self.gate('FINAL-JAR-before')
        result,args=self.guarded_apply('FINAL-JAR',self.folder/'RF32.yaml',self.folder/'RF32-plan.json',0,False,['--allow-replication-decrease'])
        assert self.snapshot('FINAL-JAR-after')['topics']==before['topics']
        self.record('FINAL-JAR','PASS','Final packaged JAR independently verifies achieved RF without writes','Saved RF decrease plan rechecked; exit0; guarded Admin writeAttempts=0',args,0)
        self.verify()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',choices=['readonly','rf','verify','audit_final'],required=True)
    parser.add_argument('--run-dir');parser.add_argument('--allow-writes',action='store_true');parser.add_argument('--resume',action='store_true')
    args=parser.parse_args()
    if args.phase=='rf' and not args.allow_writes:parser.error('Explicit --allow-writes is required')
    if args.phase!='readonly' and not args.run_dir:parser.error('--run-dir is required')
    run=ElrRun(args.run_dir)
    try:
        if args.phase!='readonly':assert all(any(r['id']==f'R{i:02d}' and r['status']=='PASS' for r in run.manifest['results']) for i in range(1,7))
        if args.resume:assert args.phase=='rf'
        if args.phase=='rf':run.rf(args.resume)
        else:getattr(run,args.phase)()
    except Exception as e:
        run.record('RUNNER-'+args.phase,'FAIL','All guards and expected outcomes hold',type(e).__name__+': '+str(e))
        raise
    previous=[r for r in run.manifest['results'] if r['id']=='RUNNER-'+args.phase]
    if previous:
        run.manifest.setdefault('runnerEvents',[]).extend(dict(r,resolved=True) for r in previous)
        run.manifest['results']=[r for r in run.manifest['results'] if r not in previous];run.persist()
    print('RUN_DIR='+str(run.folder),flush=True)
