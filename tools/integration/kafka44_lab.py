# SPDX-License-Identifier: Apache-2.0
"""Explicit, isolated Kafka 4.4 Docker lab runner. Never starts/stops containers or deletes resources."""
import argparse, datetime, hashlib, ipaddress, json, os, pathlib, secrets, socket, subprocess, sys, time
from lab_paths import lab_directory, java_executable

if not __debug__:
    raise SystemExit('Safety assertions must be enabled; do not use python -O.')

REPO=pathlib.Path(__file__).resolve().parents[2]
LAB=lab_directory('44')
ROOT=LAB/'gitops-integration'
JAR=REPO/'build/libs/kafka-gitops-all.jar'
CLUSTER='4L6g3nShT-eMCtK--X86sw'
BOOTSTRAP='127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094'
PROPS=ROOT/'client.properties'
JAVA=java_executable()
ENV={k:v for k,v in os.environ.items() if not k.startswith(('KAFKA_','CCLOUD_'))}
DOCKER=['docker','--context','desktop-linux']

def read(p):return json.loads(pathlib.Path(p).read_text(encoding='utf-8-sig'))
def save(p,obj):pathlib.Path(p).write_text(json.dumps(obj,indent=2,ensure_ascii=False),encoding='utf-8')
def capture(args,timeout=90,env=ENV):
    return subprocess.run([str(a) for a in args],cwd=REPO,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
def jsoncmd(args):
    r=capture(args)
    if r.returncode:raise RuntimeError('Read-only safety check failed: '+subprocess.list2cmdline([str(a) for a in args])+'; '+r.stderr[:300])
    return json.loads(r.stdout)

class LabRun:
    def __init__(self,run_dir=None):
        if run_dir:
            self.folder=pathlib.Path(run_dir).resolve()
            assert self.folder.is_relative_to(ROOT.resolve())
            self.manifest=read(self.folder/'manifest.json')
        else:
            runid=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))).strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(3)
            self.folder=ROOT/('run-'+runid);self.folder.mkdir(parents=True)
            prefix='gitops-it-'+runid+'-'
            self.manifest=dict(runId=runid,prefix=prefix,clusterId=CLUSTER,topics={k:prefix+k for k in ['main','configs','rf','stale']},createdTopics=[],results=[])
        self.prefix=self.manifest['prefix']
        assert self.prefix.startswith('gitops-it-') and self.manifest['clusterId']==CLUSTER
        self.persist()

    def persist(self):
        self.manifest['jarSha256']=hashlib.sha256(JAR.read_bytes()).hexdigest()
        save(self.folder/'manifest.json',self.manifest)
        save(ROOT/'latest-run.json',dict(runDir=str(self.folder),prefix=self.manifest['prefix']))

    def command(self,id,args,expected=None,env=ENV):
        started=time.monotonic();r=capture(args,env=env)
        (self.folder/(id+'.log')).write_text(r.stdout+r.stderr,encoding='utf-8')
        save(self.folder/(id+'-command.json'),dict(command=[str(a) for a in args],exitCode=r.returncode,durationSeconds=round(time.monotonic()-started,3)))
        if expected is not None and r.returncode!=expected:raise AssertionError(id+': unexpected exit '+str(r.returncode)+'; inspect '+str(self.folder/(id+'.log')))
        return r

    def snapshot(self,id):
        out=self.folder/(id+'-metadata.json')
        self.command(id+'-snapshot',[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),
                'com.devshawn.kafka.gitops.KafkaLabProbe','snapshot',PROPS,self.prefix,out],0)
        result=read(out)
        assert result['clusterId']==CLUSTER
        assert {n['id'] for n in result['nodes']}=={1,2,3}
        assert all(d['healthy'] for dirs in result['logDirectories'].values() for d in dirs)
        return result

    def gate(self,id):
        resolved={entry[4][0] for entry in socket.getaddrinfo('localhost',19092,type=socket.SOCK_STREAM)}
        assert resolved and all(ipaddress.ip_address(addr).is_loopback for addr in resolved),'localhost resolves outside loopback'
        context=capture(['docker','context','show'])
        assert context.returncode==0 and context.stdout.strip()=='desktop-linux','Wrong Docker context'
        config=jsoncmd(DOCKER+['compose','-p','kafka-gitops-lab','-f',LAB/'compose.yaml','config','--format','json'])
        assert config['name']=='kafka-gitops-lab' and set(config['services'])=={'kafka-1','kafka-2','kafka-3'}
        proof=[]
        for i in [1,2,3]:
            service=config['services']['kafka-'+str(i)];env=service['environment'];port=str(19091+i)
            assert service['image']=='apache/kafka:4.4.0'
            assert env['CLUSTER_ID']==CLUSTER and str(env['KAFKA_NODE_ID'])==str(i) and env['KAFKA_BROKER_RACK']=='rack-'+str(i)
            assert env['KAFKA_PROCESS_ROLES']=='broker,controller'
            assert service['ports']==[dict(mode='ingress',host_ip='127.0.0.1',target=9092,published=port,protocol='tcp')]
            inspected=jsoncmd(DOCKER+['inspect','kafka-gitops-lab-kafka-'+str(i)+'-1'])[0]
            labels=inspected['Config']['Labels'];actual=dict(v.split('=',1) for v in inspected['Config']['Env'])
            assert inspected['State']['Running'] and inspected['Config']['Image']=='apache/kafka:4.4.0'
            assert labels['com.docker.compose.project']=='kafka-gitops-lab' and labels['com.docker.compose.service']=='kafka-'+str(i)
            assert pathlib.Path(labels['com.docker.compose.project.working_dir']).resolve()==LAB.resolve()
            assert pathlib.Path(labels['com.docker.compose.project.config_files']).resolve()==(LAB/'compose.yaml').resolve()
            assert actual['CLUSTER_ID']==CLUSTER and actual['KAFKA_NODE_ID']==str(i) and actual['KAFKA_BROKER_RACK']=='rack-'+str(i)
            for key in ['KAFKA_PROCESS_ROLES','KAFKA_LISTENERS','KAFKA_LISTENER_SECURITY_PROTOCOL_MAP','KAFKA_CONTROLLER_QUORUM_VOTERS','KAFKA_INTER_BROKER_LISTENER_NAME']:
                assert actual[key]==str(env[key]),'Runtime config differs from Compose'
            assert set(inspected['NetworkSettings']['Networks'])=={'kafka-gitops-lab_kafka-lab'}
            assert actual['KAFKA_ADVERTISED_LISTENERS']==f'INTERNAL://kafka-{i}:19092,HOST://localhost:{port}'
            bindings={k:v for k,v in inspected['NetworkSettings']['Ports'].items() if v}
            assert bindings=={'9092/tcp':[{'HostIp':'127.0.0.1','HostPort':port}]},bindings
            proof.append(dict(name=inspected['Name'],id=inspected['Id'],image=inspected['Image'],project=labels['com.docker.compose.project'],nodeId=i,rack=actual['KAFKA_BROKER_RACK'],ports=bindings,running=True))
        assert len({p['image'] for p in proof})==1
        properties=dict(line.split('=',1) for line in PROPS.read_text().splitlines() if '=' in line)
        assert properties=={'bootstrap.servers':BOOTSTRAP,'client.id':'kafka-gitops-integration','security.protocol':'PLAINTEXT','request.timeout.ms':'10000','default.api.timeout.ms':'30000'}
        save(self.folder/(id+'-docker-proof.json'),dict(context='desktop-linux',advertisedLoopbackAddresses=sorted(resolved),composeProject=config['name'],composeSha256=hashlib.sha256((LAB/'compose.yaml').read_bytes()).hexdigest(),containers=proof))
        observed=self.snapshot(id)
        assert not any(not key.startswith(self.prefix) for key in observed['reassignments']),'Foreign active reassignment'
        return observed

    def record(self,id,status,expected,actual,command=None,exit_code=None,before=None,after=None,comment=''):
        row=dict(id=id,status=status,expected=expected,actual=actual,command=[str(a) for a in command] if command else None,
                 exitCode=exit_code,before=before,after=after,comment=comment)
        self.manifest['results']=[r for r in self.manifest['results'] if r['id']!=id]+[row]
        self.persist();print(id,status,actual,flush=True)

    def yaml(self,id,key,partitions=2,rf=1,configs=None):
        name=self.manifest['topics'][key];assert name.startswith(self.prefix)
        configs={'max.message.bytes':'1048588'} if configs is None else configs
        text=f'settings:\n  topics:\n    include:\n      prefixed: [{name}]\ntopics:\n  {name}:\n    partitions: {partitions}\n    replication: {rf}\n'
        if configs:
            text+='    configs:\n'+''.join(f'      {k}: {v}\n' for k,v in configs.items())
        p=self.folder/(id+'.yaml');p.write_text(text,encoding='utf-8');return p

    def args(self,yaml,command,no_delete=True,extra=()):
        return [JAVA,'-jar',JAR,'--command-config',PROPS,'--file',yaml,'--skip-acls']+(['--no-delete'] if no_delete else [])+[command,*extra]

    def plan(self,id,yaml,no_delete=True,expected=0,guarded=False,extra=()):
        out=self.folder/(id+'.json');args=self.args(yaml,'plan',no_delete,['--output',out,*extra])
        if guarded:
            args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.KafkaLabProbe','plan',PROPS,self.folder/(id+'-api-audit.json')]+args[3:]
        r=self.command(id,args,expected)
        if r.returncode==0:self.check_plan(read(out))
        else:assert not out.exists(),'Blocked plan exported JSON'
        if guarded:assert read(self.folder/(id+'-api-audit.json'))['writeAttempts']==0
        return out,r,args

    def check_plan(self,plan):
        assert not plan.get('aclPlans')
        for topic in plan.get('topicPlans',[]):
            assert topic['name'] in self.manifest['topics'].values() and topic['name'].startswith(self.prefix)
            assert topic['action']!='REMOVE','Refusing any topic DELETE'
            assert all(c['key'] in ['max.message.bytes','retention.ms','cleanup.policy'] for c in topic.get('topicConfigPlans',[]))
            if topic.get('replicationPlan'):assert topic['replicationPlan']['clusterId']==CLUSTER

    def apply(self,id,yaml,plan,no_delete=True,extra=(),expected=0):
        self.check_plan(read(plan));before=self.gate(id+'-before')
        args=self.args(yaml,'apply',no_delete,['--plan',plan,*extra]);r=self.command(id,args,expected)
        after=self.snapshot(id+'-after')
        assert set(after['allTopics'])-set(before['allTopics'])<=set(self.manifest['topics'].values())
        assert set(before['allTopics'])<=set(after['allTopics']),'Topic deletion detected'
        return before,after,r,args

    def independent(self,id,name):
        assert name in self.manifest['topics'].values()
        args=DOCKER+['exec','kafka-gitops-lab-kafka-1-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka-1:19092','--describe','--topic',name]
        return self.command(id+'-kafka-cli',args,0)

    def readonly(self):
        before=self.gate('R01');assert not any(t.startswith(self.prefix) for t in before['allTopics'])
        self.manifest['baselineTopics']=before['allTopics']
        self.record('R01','PASS','Correct local identity/brokers/quorum','Cluster identity, 3 brokers/racks/log dirs/quorum confirmed',exit_code=0,before=before)
        self.command('R02-help',[JAVA,'-jar',JAR,'plan','--help'],0)
        y=self.yaml('R02','main');args=self.args(y,'validate')
        self.command('R02-validate',args,0)
        self.record('R02','PASS','Help and local YAML validation','Help/config/validate exit 0',args,0)
        _,r,args=self.plan('R03',y);after=self.snapshot('R03-after')
        assert before['topics']==after['topics'] and before['allTopics']==after['allTopics']
        self.record('R03','PASS','New-topic plan without writes','ADD 2 partitions/RF1; metadata unchanged',args,r.returncode,before,after)
        _,r,args=self.plan('R04',y,guarded=True)
        self.record('R04','PASS','Read-only Admin audit','Zero write attempts, real Admin reads delegated',args,r.returncode)
        env=dict(ENV,KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:19094',KAFKA_CLIENT_ID='ignored-environment-client')
        args=[JAVA,'-jar',JAR,'-v','-c',PROPS,'-f',y,'--skip-acls','--no-delete','plan','-o',self.folder/'R05.json']
        r=self.command('R05',args,0,env)
        assert BOOTSTRAP in r.stdout.replace(', ', ',') and 'ignored-environment-client' not in r.stdout
        self.record('R05','PASS','Properties file beats conflicting environment','Explicit three loopback endpoints/client.id retained',args,0)
        for i in [1,2,3]:self.command('R06-version-'+str(i),DOCKER+['exec',f'kafka-gitops-lab-kafka-{i}-1','/opt/kafka/bin/kafka-topics.sh','--version'],0)
        self.command('R06-quorum',DOCKER+['exec','kafka-gitops-lab-kafka-1-1','/opt/kafka/bin/kafka-metadata-quorum.sh','--bootstrap-server','kafka-1:19092','describe','--status'],0)
        self.record('R06','PASS','Independent Kafka CLI version/quorum','All broker binaries and quorum independently read',exit_code=0)

    def change(self,id,key,partitions=2,rf=1,configs=None):
        y=self.yaml(id,key,partitions,rf,configs);p,planned,_=self.plan(id+'-plan',y)
        before,after,r,args=self.apply(id,y,p)
        topic=after['topics'][self.manifest['topics'][key]]
        assert len(topic['partitions'])==partitions and all(len(p['replicas'])==rf and set(p['replicas'])==set(p['isr']) and p['leader'] in p['isr'] for p in topic['partitions'])
        for k,v in (configs or {'max.message.bytes':'1048588'}).items():assert topic['configs'][k]['value']==str(v)
        self.independent(id,self.manifest['topics'][key])
        name=self.manifest['topics'][key]
        if name not in self.manifest['createdTopics']:self.manifest['createdTopics'].append(name)
        self.record(id,'PASS',f'Partitions={partitions},RF={rf},configs={configs}', 'Apply verified through Admin + Kafka CLI',args,r.returncode,before,after)
        return y,p,after

    def basic(self):
        y,p,_=self.change('W01','main')
        _,r,args=self.plan('W02',y);assert 'NO_CHANGES' in r.stdout
        self.record('W02','PASS','No changes after creation','NO_CHANGES',args,0)
        self.change('W03','configs')
        self.change('W04','configs',configs={'max.message.bytes':'1048588','retention.ms':'3600000'})
        y,p,_=self.change('W05','configs',configs={'max.message.bytes':'2097152','retention.ms':'3600000'})
        changes=read(p)['topicPlans'][0]['topicConfigPlans'];assert any(c['key']=='max.message.bytes' and c['previousValue']=='1048588' for c in changes)
        y,p,after=self.change('W06','configs',configs={'max.message.bytes':'2097152'})
        assert after['topics'][self.manifest['topics']['configs']]['configs']['retention.ms']['source']!='DYNAMIC_TOPIC_CONFIG'
        self.change('W07','main',partitions=4)
        self.change('W08','rf')
        self.change('W09','stale')

    def rf(self):
        before=self.gate('RF01-before');y=self.yaml('RF01','rf',rf=2)
        p,r,args=self.plan('RF01-plan',y,expected=2,guarded=True)
        assert 'ELR_UNSUPPORTED' in r.stdout
        assert 'Replication factor: 1 -> 2' in r.stdout or 'Replication: [1] -> 2' in r.stdout
        cmd=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.KafkaLabProbe','plan',PROPS,self.folder/'RF01-apply-api-audit.json']+self.args(y,'apply')[3:]
        self.gate('RF01-apply-before');applied=self.command('RF01-apply',cmd,2)
        assert 'ELR_UNSUPPORTED' in applied.stdout
        assert read(self.folder/'RF01-apply-api-audit.json')['writeAttempts']==0
        after=self.snapshot('RF01-after');assert before['topics']==after['topics']
        self.record('RF01','BLOCKED','Real RF 1 -> 2 after fresh preflight','ELR enabled; no executable JSON; zero guarded write attempts',cmd,2,before,after)
        for id,expected in [('RF02','Real RF 2 -> 3'),('RF03','Real RF 3 -> 2 with allow')]:
            self.record(id,'BLOCKED',expected,'Prerequisite RF not reached because RF01 is blocked',comment='Not executed; no ELR/feature/policy changes authorized or performed')
        p,r,args=self.plan('R07-plan',y,guarded=True,extra=['--no-manage-replication'])
        assert 'RF_MANAGEMENT_DISABLED' in r.stdout and not read(p).get('topicPlans')
        assert self.snapshot('R07-after')['topics']==after['topics']
        self.record('R07','PASS','Explicit RF opt-out remains read-only','Exit0; RF drift warning; no RF operation or write attempts',args,0,after,after)

    def negative(self):
        before=self.gate('N01-before');y=self.yaml('N01','configs',configs={'max.message.bytes':'-1'});p,_,_=self.plan('N01-plan',y)
        b,a,r,args=self.apply('N01',y,p,expected=2)
        assert b['topics']==a['topics'];self.record('N01','PASS','Invalid config rejected','Exit 2, config metadata unchanged',args,2,b,a)
        y=self.yaml('N02','main',partitions=2);_,r,args=self.plan('N02-plan',y,expected=2)
        a=self.snapshot('N02-after');assert len(a['topics'][self.manifest['topics']['main']]['partitions'])==4
        self.record('N02','PASS','Partition decrease refused','4 -> 2 blocked, actual count remains 4',args,2,after=a)
        y=self.yaml('N03','rf',rf=4);_,r,args=self.plan('N03-plan',y,expected=2)
        self.record('N03','BLOCKED','Isolate insufficient-broker rejection for RF4','Desired RF4/brokers3 known, but ELR blocks before placement calculation',args,2,comment='No export/writes; the insufficient-broker rule itself was not reached')
        y=self.yaml('N04','stale',partitions=3);p,_,_=self.plan('N04-stale-plan',y)
        self.change('N04-fresh-increase','stale',partitions=4)
        b,a,r,args=self.apply('N04-stale-apply',y,p,expected=2);assert b['topics']==a['topics']
        self.record('N04','PASS','Stale partition JSON rejected','Saved previous2/target3, actual4; exit2, metadata unchanged',args,2,b,a)
        for id,args in [
            ('N05',[JAVA,'-jar',JAR,'plan','--manage-replication','--no-manage-replication']),
            ('N06',[JAVA,'-jar',JAR,'-c',self.folder/'missing.properties','validate'])]:
            r=self.command(id,args,2);self.record(id,'PASS','Safe local rejection','Exit2, no apply command',args,2)
        for id,content in [('N07','topics: [ invalid'),('N08',f'topics:\n  {self.prefix}invalid:\n    partitions: 0\n    replication: 1\n'),('N09',f'topics:\n  {self.prefix}invalid:\n    partitions: 1\n    replication: -1\n')]:
            f=self.folder/(id+'.yaml');f.write_text(content,encoding='utf-8');args=self.args(f,'validate');self.command(id,args,2);self.record(id,'PASS','Invalid YAML/state rejected','Exit2, local validation only',args,2)
        empty=self.folder/'no-bootstrap.properties';empty.write_text('security.protocol=PLAINTEXT\n',encoding='utf-8')
        y=self.yaml('N10','main',partitions=4);args=self.args(y,'plan');args[args.index(PROPS)]=empty
        r=self.command('N10',args,2);assert 'BOOTSTRAP_MISSING' in r.stdout;self.record('N10','PASS','No implicit localhost','BOOTSTRAP_MISSING before connection',args,2)
        port=self.folder/'unavailable.properties';port.write_text('bootstrap.servers=127.0.0.1:19999\nsecurity.protocol=PLAINTEXT\nrequest.timeout.ms=500\ndefault.api.timeout.ms=1000\n',encoding='utf-8')
        args=self.args(y,'plan');args[args.index(PROPS)]=port;r=self.command('N11',args,2);assert 'TIMEOUT' in r.stdout
        self.record('N11','PASS','Bounded unavailable loopback port','TIMEOUT, exit2; no external endpoint',args,2)
        for id,label in [('N12','Decrease without permission'),('N13','Decrease with no-delete'),('N14','Unconfirmed broker eligibility / ISR loss'),('N15','ACK vs RUNNING during a real RF transfer')]:
            self.record(id,'NOT_TESTED',label,'Cannot isolate this live RF check while ELR blocks RF execution',comment='Unit coverage retained; no broker faults or policy bypass performed')
        final=self.gate('FINAL')
        baseline=self.manifest.get('baselineTopics',next(r['before']['allTopics'] for r in self.manifest['results'] if r['id']=='R01'))
        assert set(final['allTopics'])==set(baseline)|set(self.manifest['createdTopics'])
        cleanup=['# NOT EXECUTED. Re-run full identity/Docker gate before each explicitly authorized deletion.']
        for name in self.manifest['createdTopics']:
            cleanup.append(subprocess.list2cmdline(['docker','--context','desktop-linux','exec','kafka-gitops-lab-kafka-1-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka-1:19092','--delete','--topic',name]))
        (self.folder/'cleanup-NOT-EXECUTED.txt').write_text('\n'.join(cleanup)+'\n',encoding='utf-8')
        self.persist()

    def verify(self):
        observed=self.gate('VERIFY')
        reference=read(self.folder/'FINAL-metadata.json')
        for name,t in observed['topics'].items():
            assert name in self.manifest['createdTopics'],'Unknown resource in test namespace'
            assert name in reference['topics'] and t['topicId']==reference['topics'][name]['topicId'],'Topic UUID changed'
        print('Identity/containers/endpoints/topic UUIDs verified; no Kafka writes performed.',flush=True)

    def recheck(self):
        before=self.gate('RECHECK-before')
        for id,file,label in [('R08','W06.yaml','Config final-state plan'),('R09','W07.yaml','Partition final-state plan')]:
            p,r,args=self.plan(id+'-plan',self.folder/file,guarded=True)
            assert 'NO_CHANGES' in r.stdout and not read(p).get('topicPlans')
            after=self.snapshot(id+'-after');assert after['topics']==before['topics']
            self.record(id,'PASS','No changes after successful apply',label+': NO_CHANGES, zero write attempts',args,0,before,after)
        self.gate('W10-before')
        args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.KafkaLabProbe','plan',PROPS,self.folder/'W10-api-audit.json']+self.args(self.folder/'W07.yaml','apply')[3:]
        result=self.command('W10',args,0)
        assert read(self.folder/'W10-api-audit.json')['writeAttempts']==0
        assert ('Status: NO_CHANGES' in result.stdout
                or 'There are no necessary changes; the actual state matches the desired state.' in result.stdout)
        after=self.snapshot('W10-after');assert after['topics']==before['topics']
        self.record('W10','PASS','Reapply achieved desired state without writes','No-op apply exit0; zero write attempts; metadata unchanged',args,0,before,after)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',choices=['readonly','basic','rf','negative','recheck','verify'],required=True)
    parser.add_argument('--run-dir');parser.add_argument('--allow-writes',action='store_true')
    args=parser.parse_args()
    if args.phase not in ['readonly','verify'] and not args.allow_writes:parser.error('Explicit --allow-writes is required for this phase.')
    if args.phase=='verify' and not args.run_dir:parser.error('--run-dir is required for verification.')
    run=LabRun(args.run_dir)
    try:
        if args.phase!='readonly':
            assert all(any(r['id']==f'R{i:02d}' and r['status']=='PASS' for r in run.manifest['results']) for i in range(1,7)), 'Read-only acceptance must pass first'
        getattr(run,args.phase)()
    except Exception as e:
        run.record('RUNNER-'+args.phase,'FAIL','All guards and expected outcomes hold',type(e).__name__+': '+str(e))
        raise
    resolved=[r for r in run.manifest['results'] if r['id']=='RUNNER-'+args.phase or r['id']=='RUNNER-readonly'
              and all(any(row['id']==f'R{i:02d}' and row['status']=='PASS' for row in run.manifest['results']) for i in range(1,7))]
    if resolved:
        run.manifest.setdefault('runnerEvents',[]).extend(dict(r,resolved=True) for r in resolved)
        run.manifest['results']=[r for r in run.manifest['results'] if r not in resolved]
        run.persist()
    print('RUN_DIR='+str(run.folder),flush=True)
