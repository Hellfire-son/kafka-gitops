# SPDX-License-Identifier: Apache-2.0
"""Separate, opt-in Kafka 3.x runner. Does not alter Kafka 4.4 gates or start/stop/delete resources."""
import argparse, datetime, hashlib, ipaddress, json, os, pathlib, secrets, socket, subprocess, sys, time
from lab_paths import lab_directory, java_executable, bind_source_paths

if not __debug__:
    raise SystemExit('Safety assertions must be enabled; do not use python -O.')

REPO=pathlib.Path(__file__).resolve().parents[2]
JAR=REPO/'build/libs/kafka-gitops-all.jar'
PROFILE=None

def select_profile(version):
    global PROFILE,LAB,ROOT,PROPS,CLUSTER,BOOTSTRAP,PROJECT,BASE_PORT,IMAGE
    assert version in ('371','323'),'Unknown legacy lab'
    PROFILE=version;PROJECT='kafka-gitops-'+version+'-lab'
    LAB=lab_directory(version);ROOT=LAB/'integration';PROPS=ROOT/'client.properties'
    BASE_PORT={'371':23791,'323':23291}[version]
    BOOTSTRAP=','.join('127.0.0.1:'+str(BASE_PORT+i) for i in (1,2,3))
    CLUSTER='GV57l31pRMa-yIjZQBWyww' if version=='371' else None
    IMAGE='apache/kafka:3.7.1' if version=='371' else 'kafka-gitops-lab/kafka:3.2.3-verified'

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

def verify_config_bind(binding,filename):
    assert PROFILE=='323' and filename in ['broker-1.properties','broker-2.properties','broker-3.properties','zookeeper.properties']
    config=LAB/'config'/filename
    allowed=bind_source_paths(str(config.resolve()))
    assert binding['Source'] in allowed and binding['Destination']=='/opt/lab/config/'+filename and not binding['RW']

class LegacyRun:
    def __init__(self,run_dir=None):
        global CLUSTER
        assert PROFILE in ('371','323')
        self.identity=read(LAB/'identity.json')
        assert self.identity['project']==PROJECT and self.identity['version']=={'371':'3.7.1','323':'3.2.3'}[PROFILE]
        if PROFILE=='371':assert self.identity['clusterId']==CLUSTER
        else:CLUSTER=self.identity['clusterId']
        assert CLUSTER and CLUSTER!='4L6g3nShT-eMCtK--X86sw'
        if run_dir:
            self.folder=pathlib.Path(run_dir).resolve()
            assert self.folder.is_relative_to(ROOT.resolve())
            self.manifest=read(self.folder/'manifest.json');assert self.manifest.get('legacyVersion')==PROFILE
        else:
            runid=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))).strftime('%Y%m%d-%H%M%S')+'-'+secrets.token_hex(3)
            self.folder=ROOT/('run-'+runid);self.folder.mkdir(parents=True)
            prefix='gitops-it-'+runid+'-'
            self.manifest=dict(runId=runid,prefix=prefix,clusterId=CLUSTER,topics={k:prefix+k for k in ['main','configs','rf','stale']},createdTopics=[],results=[])
        self.manifest['legacyVersion']=PROFILE
        self.prefix=self.manifest['prefix']
        assert self.prefix.startswith('gitops-it-') and self.manifest['clusterId']==CLUSTER
        self.persist()

    def persist(self):
        actual=hashlib.sha256(JAR.read_bytes()).hexdigest()
        assert self.manifest.setdefault('jarSha256',actual)==actual,'JAR changed: use a new run after regression'
        save(self.folder/'manifest.json',self.manifest)
        save(ROOT/'latest-run.json',dict(runDir=str(self.folder),prefix=self.manifest['prefix']))

    def command(self,id,args,expected=None,env=ENV):
        assert hashlib.sha256(JAR.read_bytes()).hexdigest()==self.manifest['jarSha256'],'JAR changed before command; start a new run'
        started=time.monotonic();r=capture(args,env=env)
        (self.folder/(id+'.log')).write_text(r.stdout+r.stderr,encoding='utf-8')
        save(self.folder/(id+'-command.json'),dict(command=[str(a) for a in args],exitCode=r.returncode,durationSeconds=round(time.monotonic()-started,3)))
        if expected is not None and r.returncode!=expected:raise AssertionError(id+': unexpected exit '+str(r.returncode)+'; inspect '+str(self.folder/(id+'.log')))
        return r

    def snapshot(self,id):
        out=self.folder/(id+'-metadata.json')
        self.command(id+'-snapshot',[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),
                'com.devshawn.kafka.gitops.Kafka3xLabProbe',PROFILE+'-snapshot',PROPS,CLUSTER,self.prefix,out],0)
        result=read(out)
        assert result['clusterId']==CLUSTER
        assert {n['id'] for n in result['nodes']}=={1,2,3}
        assert all(d['healthy'] for dirs in result['logDirectories'].values() for d in dirs)
        return result

    def docker_gate(self,id):
        assert capture(['docker','context','show']).stdout.strip()=='desktop-linux'
        resolved={entry[4][0] for entry in socket.getaddrinfo('localhost',BASE_PORT+1,type=socket.SOCK_STREAM)}
        assert resolved and all(ipaddress.ip_address(a).is_loopback for a in resolved)
        assert hashlib.sha256((LAB/'compose.yaml').read_bytes()).hexdigest()==self.identity['composeSha256']
        c=jsoncmd(DOCKER+['compose','-p',PROJECT,'-f',LAB/'compose.yaml','config','--format','json'])
        expected={'kafka-1','kafka-2','kafka-3'}|({'zookeeper'} if PROFILE=='323' else set())
        assert c['name']==PROJECT and set(c['services'])==expected
        network=jsoncmd(DOCKER+['network','inspect',PROJECT+'_legacy-host'])[0]
        assert network['Driver']=='bridge' and network['Options']['com.docker.network.bridge.enable_ip_masquerade']=='false'
        assert network['Labels']['com.docker.compose.project']==PROJECT
        proof=[]
        for service in sorted(expected):
            i=int(service[-1]) if service.startswith('kafka-') else None
            container=jsoncmd(DOCKER+['inspect',PROJECT+'-'+service+'-1'])[0]
            labels=container['Config']['Labels'];actual=dict(v.split('=',1) for v in container['Config']['Env'])
            assert container['State']['Running'] and container['Config']['Image']==IMAGE
            assert not container['HostConfig']['Privileged'] and not container['HostConfig']['PidMode']
            assert container['Image']==self.identity['image'] and labels['com.docker.compose.project']==PROJECT
            assert labels['com.docker.compose.service']==service
            assert pathlib.Path(labels['com.docker.compose.project.working_dir']).resolve()==LAB.resolve()
            assert pathlib.Path(labels['com.docker.compose.project.config_files']).resolve()==(LAB/'compose.yaml').resolve()
            assert set(container['NetworkSettings']['Networks'])=={PROJECT+'_legacy-host'}
            ports={k:v for k,v in container['NetworkSettings']['Ports'].items() if v}
            assert ports==({'9092/tcp':[{'HostIp':'127.0.0.1','HostPort':str(BASE_PORT+i)}]} if i else {})
            data=[v for v in container['Mounts'] if v['Type']=='volume']
            expected_volume=PROJECT+'_'+(service+'-data')
            expected_mounts={'/var/lib/kafka/data':expected_volume} if i else {'/var/lib/zookeeper':expected_volume}
            if PROFILE=='371':expected_mounts.update(self.identity['auxiliaryVolumes'][service])
            assert {v['Destination']:v['Name'] for v in data}==expected_mounts
            volume=jsoncmd(DOCKER+['volume','inspect',expected_volume])[0]
            assert volume['Labels']['com.docker.compose.project']==PROJECT
            if PROFILE=='371':
                assert all(v['Type']=='volume' for v in container['Mounts'])
                assert actual['CLUSTER_ID']==CLUSTER and actual['KAFKA_NODE_ID']==str(i) and actual['KAFKA_BROKER_RACK']=='rack-'+str(i)
                for key in ['KAFKA_PROCESS_ROLES','KAFKA_LISTENERS','KAFKA_ADVERTISED_LISTENERS','KAFKA_CONTROLLER_QUORUM_VOTERS','KAFKA_LISTENER_SECURITY_PROTOCOL_MAP','KAFKA_INTER_BROKER_LISTENER_NAME','KAFKA_LOG_DIRS']:
                    assert actual[key]==str(c['services'][service]['environment'][key])
                assert actual['KAFKA_PROCESS_ROLES']=='broker,controller'
                assert actual['KAFKA_ADVERTISED_LISTENERS']==f'INTERNAL://kafka-{i}:19092,HOST://127.0.0.1:{BASE_PORT+i}'
            else:
                config=LAB/'config'/('broker-'+str(i)+'.properties' if i else 'zookeeper.properties')
                assert hashlib.sha256(config.read_bytes()).hexdigest()==self.identity['configSha256'][config.name]
                binds=[v for v in container['Mounts'] if v['Type']=='bind']
                assert len(binds)==1;verify_config_bind(binds[0],config.name)
                expected_cmd=['/opt/kafka/bin/'+('kafka-server-start.sh' if i else 'zookeeper-server-start.sh'),'/opt/lab/config/'+config.name]
                assert container['Config']['Cmd']==expected_cmd
            proof.append(dict(name=container['Name'],id=container['Id'],image=container['Image'],ports=ports,volume=expected_volume))
        props=dict(line.split('=',1) for line in PROPS.read_text().splitlines() if '=' in line)
        assert props=={'bootstrap.servers':BOOTSTRAP,'client.id':'kafka-gitops-'+PROFILE+'-integration','security.protocol':'PLAINTEXT','request.timeout.ms':'10000','default.api.timeout.ms':'30000'}
        save(self.folder/(id+'-docker-proof.json'),dict(context='desktop-linux',project=PROJECT,clusterId=CLUSTER,containers=proof,composeSha256=self.identity['composeSha256']))

    def gate(self,id):
        self.docker_gate(id)
        observed=self.snapshot(id)
        assert not observed['reassignments'],'Competing reassignment'
        assert all(c['min.insync.replicas']=='1' for c in observed['brokerConfigs'].values())
        if PROFILE=='371':assert observed['finalizedFeatures']['metadata.version']['max']<23
        else:assert all(c['zookeeper.connect']=='zookeeper:2181' and c['process.roles']=='' for c in observed['brokerConfigs'].values())
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

    def plan(self,id,yaml,no_delete=True,expected=0,guarded=True,extra=()):
        out=self.folder/(id+'.json');args=self.args(yaml,'plan',no_delete,['--output',out,*extra])
        if guarded:
            args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.Kafka3xLabProbe',PROFILE+'-plan',PROPS,CLUSTER,self.folder/(id+'-api-audit.json')]+args[3:]
        r=self.command(id,args,expected)
        if r.returncode==0:self.check_plan(read(out))
        else:assert not out.exists(),'Blocked plan exported JSON'
        if guarded:assert read(self.folder/(id+'-api-audit.json'))['writeAttempts']==0
        return out,r,args

    def check_plan(self,plan):
        assert not plan.get('aclPlans')
        for topic in plan.get('topicPlans',[]):
            assert topic['name'] in self.manifest['topics'].values() and topic['name'].startswith(self.prefix)
            assert topic['action'] in ['ADD','UPDATE','NO_CHANGE'],'Refusing topic DELETE or unknown actions'
            assert all(c['key'] in ['max.message.bytes','retention.ms','cleanup.policy'] for c in topic.get('topicConfigPlans',[]))
            assert all(k in ['max.message.bytes','retention.ms','cleanup.policy'] for k in topic.get('topicDetails',{}).get('configs',{}))
            if topic.get('replicationPlan'):assert topic['replicationPlan']['clusterId']==CLUSTER

    def apply(self,id,yaml,plan,no_delete=True,extra=(),expected=0):
        self.check_plan(read(plan));before=self.gate(id+'-before')
        self.manifest.setdefault('writeJarSha256',hashlib.sha256(JAR.read_bytes()).hexdigest())
        args=self.args(yaml,'apply',no_delete,['--plan',plan,*extra]);r=self.command(id,args,expected)
        after=self.snapshot(id+'-after')
        assert set(after['allTopics'])-set(before['allTopics'])<=set(self.manifest['topics'].values())
        assert set(before['allTopics'])<=set(after['allTopics']),'Topic deletion detected'
        return before,after,r,args

    def independent(self,id,name):
        assert name in self.manifest['topics'].values()
        args=DOCKER+['exec',PROJECT+'-kafka-1-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka-1:19092','--describe','--topic',name]
        return self.command(id+'-kafka-cli',args,0)

    def readonly(self):
        before=self.gate('R01');assert not any(t.startswith(self.prefix) for t in before['allTopics'])
        self.manifest['baselineTopics']=before['allTopics']
        self.record('R01','PASS','Correct local identity/brokers/metadata mode','Cluster identity, 3 brokers/racks/log dirs and metadata mode confirmed',exit_code=0,before=before)
        self.command('R02-help',[JAVA,'-jar',JAR,'plan','--help'],0)
        y=self.yaml('R02','main');args=self.args(y,'validate')
        self.command('R02-validate',args,0)
        self.record('R02','PASS','Help and local YAML validation','Help/config/validate exit 0',args,0)
        _,r,args=self.plan('R03',y);after=self.snapshot('R03-after')
        assert before['topics']==after['topics'] and before['allTopics']==after['allTopics']
        self.record('R03','PASS','New-topic plan without writes','ADD 2 partitions/RF1; metadata unchanged',args,r.returncode,before,after)
        _,r,args=self.plan('R04',y,guarded=True)
        self.record('R04','PASS','Read-only Admin audit','Zero write attempts, real Admin reads delegated',args,r.returncode)
        env=dict(ENV,KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:'+str(BASE_PORT+3),KAFKA_CLIENT_ID='ignored-environment-client')
        args=[JAVA,'-jar',JAR,'-v','-c',PROPS,'-f',y,'--skip-acls','--no-delete','plan','-o',self.folder/'R05.json']
        r=self.command('R05',args,0,env)
        assert BOOTSTRAP in r.stdout.replace(', ', ',') and 'ignored-environment-client' not in r.stdout
        self.record('R05','PASS','Properties file beats conflicting environment','Explicit three loopback endpoints/client.id retained',args,0)
        for i in [1,2,3]:self.command('R06-version-'+str(i),DOCKER+['exec',PROJECT+f'-kafka-{i}-1','/opt/kafka/bin/kafka-topics.sh','--version'],0)
        self.command('R06-api-versions',DOCKER+['exec',PROJECT+'-kafka-1-1','/opt/kafka/bin/kafka-broker-api-versions.sh','--bootstrap-server','kafka-1:19092'],0)
        self.record('R06','PASS','Independent broker versions and API negotiation','All 3 broker CLI versions and broker API ranges saved',exit_code=0)
        save(self.folder/'prior-topics-baseline.json',self.prior_topics('R07'))
        self.record('R07','PASS','Preserve earlier test namespaces','Prior UUID/config/assignment baseline saved',exit_code=0)

    def prior_topics(self,id):
        out=self.folder/(id+'-prior-topics.json')
        self.command(id+'-prior-snapshot',[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),
                     'com.devshawn.kafka.gitops.Kafka3xLabProbe',PROFILE+'-snapshot',PROPS,CLUSTER,'gitops-it-',out],0)
        return {n:t for n,t in read(out)['topics'].items() if not n.startswith(self.prefix)}

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

    def guarded_apply(self,id,yaml,plan,expected=0,no_delete=True,extra=()):
        self.check_plan(read(plan));before=self.gate(id+'-before')
        args=[JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.Kafka3xLabProbe',
              PROFILE+'-plan',PROPS,CLUSTER,self.folder/(id+'-api-audit.json')]+self.args(yaml,'apply',no_delete,['--plan',plan,*extra])[3:]
        result=self.command(id,args,expected)
        assert read(self.folder/(id+'-api-audit.json'))['writeAttempts']==0
        after=self.snapshot(id+'-after');assert before['topics']==after['topics']
        return result,args

    def data_probe(self,mode,output,*extra):
        return [JAVA,'-cp',str(ROOT/'probe-classes')+os.pathsep+str(JAR),'com.devshawn.kafka.gitops.Kafka3xDataProbe',
                PROFILE+'-'+mode,PROPS,CLUSTER,self.prefix,self.manifest['topics']['rf'],output,*extra]

    def check_data(self,id):
        out=self.folder/(id+'-data.json');args=self.data_probe('check-data',out,self.folder/'seed-records.json')
        self.command(id+'-data',args,0);assert read(out)['matched']==12
        self.record(id+'-DATA','PASS','Preserve messages and offsets','12/12 keys, values, partitions and offsets preserved; no consumer commits',args,0)

    def check_rf_plan(self,plan,before,target):
        self.check_plan(plan)
        assert len(plan['topicPlans'])==1
        t=plan['topicPlans'][0];assert t['name']==self.manifest['topics']['rf'] and t['action']=='UPDATE'
        assert not t.get('topicConfigPlans') and not t.get('partitionPlan')
        actual=before['topics'][t['name']];rp=t['replicationPlan']
        assert rp['schemaVersion']==1 and rp['clusterId']==CLUSTER and rp['topicId']==actual['topicId']
        assert rp['partitionCount']==len(actual['partitions'])==2 and rp['targetReplicationFactor']==target
        assert rp['expectedMinInSyncReplicas']==1 and {b['id'] for b in rp['brokerSnapshot']}=={1,2,3}
        details=t.get('topicDetails')
        if details:
            assert details.get('partitions')==2 and details.get('replication')==target
            assert all(k in actual['configs'] and str(v)==actual['configs'][k]['value'] for k,v in details.get('configs',{}).items())
        assert len(rp['assignments'])==2 and {a['partition'] for a in rp['assignments']}=={0,1}
        for a in rp['assignments']:
            p=next(p for p in actual['partitions'] if p['id']==a['partition'])
            assert a['previousReplicas']==p['replicas'] and set(p['isr'])==set(p['replicas'])
            assert p['leader'] in a['targetReplicas'] and len(a['targetReplicas'])==len(set(a['targetReplicas']))==target
            assert set(a['targetReplicas'])<={1,2,3}
            if target>len(p['replicas']):assert a['targetReplicas'][:len(p['replicas'])]==p['replicas']
            else:assert set(a['targetReplicas'])<=set(p['isr'])
        return rp

    def transfer(self,id,target,decrease=False):
        before=self.gate(id+'-plan-before');y=self.yaml(id,'rf',rf=target)
        flags=['--allow-replication-decrease'] if decrease else []
        p,r,args=self.plan(id+'-plan',y,no_delete=not decrease,expected=None,extra=flags)
        if r.returncode!=0:
            self.record(id,'BLOCKED','RF target '+str(target)+' reaches actual completion',r.stdout[-1800:],args,r.returncode,before=before)
            return None
        assert 'ELR feature: NOT_APPLICABLE' in r.stdout,'Positive non-applicability proof required'
        rp=self.check_rf_plan(read(p),before,target)
        if decrease:
            for label,no_delete,extra in [('N02',False,[]),('N03',True,flags)]:
                refused,cmd=self.guarded_apply(label,y,p,2,no_delete,extra)
                self.record(label,'PASS','Decrease permission/no-delete guard rejects before writes','Exit2; zero Admin write attempts; metadata unchanged',cmd,2)
        b,after,result,cmd=self.apply(id+'-apply',y,p,no_delete=not decrease,extra=flags,expected=None)
        if result.returncode!=0:
            self.record(id,'BLOCKED','CLI verifies complete RF transfer',result.stdout[-1800:],cmd,result.returncode,b,after)
            return None
        actual=after['topics'][self.manifest['topics']['rf']]
        assert actual['topicId']==rp['topicId'] and not after['reassignments'] and 'COMPLETED' in result.stdout
        for a in rp['assignments']:
            part=next(p for p in actual['partitions'] if p['id']==a['partition'])
            assert part['replicas']==a['targetReplicas'] and set(part['isr'])==set(part['replicas']) and part['leader'] in part['isr']
            assert part['elr']==part['lastKnownElr']=='UNKNOWN','Legacy missing ELR must remain absent, not invented empty'
        self.independent(id,self.manifest['topics']['rf'])
        self.record(id,'PASS','RF target '+str(target)+' reaches terminal proof','Exact ordered target/full ISR/leader/UUID/no active; ELR NOT_APPLICABLE',cmd,0,b,after)
        self.check_data(id)
        _,again,cmd=self.plan(id+'-repeat-plan',y);assert 'NO_CHANGES' in again.stdout
        self.record(id+'-REPLAN','PASS','No-op plan','NO_CHANGES, zero guarded write attempts',cmd,0)
        _,cmd=self.guarded_apply(id+'-repeat-apply',y,p,0,not decrease,flags)
        self.record(id+'-REAPPLY','PASS','Saved achieved RF plan is no-op','Exit0, zero guarded write attempts',cmd,0)
        return y,p,after

    def rf(self):
        assert all(any(r['id']==id and r['status']=='PASS' for r in self.manifest['results']) for id in ['W08','W09'])
        assert not (self.folder/'seed-records.json').exists(),'Inspect partial state before any manual resume'
        self.gate('DATA-SEED-before');args=self.data_probe('seed',self.folder/'seed-records.json','--allow-writes')
        self.command('DATA-SEED',args,0);self.record('DATA-SEED','PASS','Seed only exact run RF topic','12 synthetic records, acks=all/idempotence',args,0)
        self.check_data('SEED')
        first=self.transfer('RF12',2)
        second=self.transfer('RF23',3) if first else None
        if second:
            r,args=self.guarded_apply('N04-RF',first[0],first[1],2)
            self.record('N04-RF','PASS','Stale RF assignments rejected at actual RF3','Saved previous RF1/targetRF2 refused; zero write attempts',args,2)
            self.transfer('RF32',2,True)
        else:
            for id in ['RF23','RF32']:
                if not any(r['id']==id for r in self.manifest['results']):self.record(id,'BLOCKED','RF prerequisite reached','Earlier RF operation blocked; no bypass or dependent write')

    def negative(self):
        y=self.yaml('N01','rf',rf=4);_,r,args=self.plan('N01-plan',y,expected=2)
        assert 'INSUFFICIENT_BROKERS' in r.stdout
        self.record('N01','PASS','RF greater than broker count rejected','INSUFFICIENT_BROKERS; no JSON; zero guarded write attempts',args,2)
        y=self.yaml('N05','main',partitions=2);_,r,args=self.plan('N05-plan',y,expected=2)
        self.record('N05','PASS','Partition decrease refused','4->2 refused; no JSON; zero guarded write attempts',args,2)
        y=self.yaml('N06','configs',configs={'max.message.bytes':'-1'});p,_,_=self.plan('N06-plan',y)
        before,after,r,args=self.apply('N06',y,p,expected=2);assert before['topics']==after['topics']
        self.record('N06','PASS','Invalid config rejected by broker','Exit2; intended invalid config request rejected, metadata unchanged',args,2,before,after)
        y=self.yaml('N07','stale',partitions=3);p,_,_=self.plan('N07-stale-plan',y)
        self.change('N07-fresh','stale',partitions=4)
        r,args=self.guarded_apply('N07-stale-apply',y,p,2)
        self.record('N07','PASS','Stale partition JSON refused','Saved previous2/target3, actual4; exit2; zero write attempts',args,2)
        unavailable=self.folder/'unavailable.properties'
        unavailable.write_text('bootstrap.servers=127.0.0.1:29999\nsecurity.protocol=PLAINTEXT\nrequest.timeout.ms=500\ndefault.api.timeout.ms=1000\n')
        y=self.yaml('N08','main',partitions=4);args=self.args(y,'plan');args[args.index(PROPS)]=unavailable
        r=self.command('N08',args,2);assert 'TIMEOUT' in r.stdout
        self.record('N08','PASS','Bounded unreachable loopback bootstrap','TIMEOUT exit2; no external endpoint',args,2)
        self.record('N09','NOT_TESTED','Authorization failure if applicable','PLAINTEXT lab without authorizer; ACL semantics/profiles not changed')
        logs='\n'.join(p.read_text(encoding='utf-8') for p in self.folder.glob('*.log'))
        assert not any(x in logs for x in ['sasl.jaas.config=','-----BEGIN PRIVATE KEY-----','ignored-environment-client'])
        self.record('N10','PASS','No secrets in logs','No credential markers; only loopback/synthetic lab properties used',exit_code=0)
        self.verify()

    def verify(self):
        final=self.gate('FINAL')
        assert set(final['allTopics'])==set(self.manifest['baselineTopics'])|set(self.manifest['createdTopics'])
        if (self.folder/'prior-topics-baseline.json').exists():assert self.prior_topics('FINAL')==read(self.folder/'prior-topics-baseline.json')
        for name in self.manifest['createdTopics']:
            first=next(r['after']['topics'][name] for r in self.manifest['results'] if r.get('after') and name in r['after']['topics'])
            assert first['topicId']==final['topics'][name]['topicId']
        self.record('FINAL','PASS','Leave all test resources intact','All brokers healthy; no active reassignment; namespaces/UUIDs retained; no cleanup',exit_code=0,after=final)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version',choices=['371','323'],required=True)
    parser.add_argument('--phase',choices=['readonly','basic','rf','negative','verify'],required=True)
    parser.add_argument('--run-dir');parser.add_argument('--allow-writes',action='store_true')
    args=parser.parse_args()
    if args.phase in ['basic','rf','negative'] and not args.allow_writes:parser.error('Explicit --allow-writes is required')
    if args.phase!='readonly' and not args.run_dir:parser.error('--run-dir is required')
    select_profile(args.version);run=LegacyRun(args.run_dir)
    try:
        if args.phase!='readonly':assert all(any(r['id']==f'R{i:02d}' and r['status']=='PASS' for r in run.manifest['results']) for i in range(1,7))
        getattr(run,args.phase)()
    except Exception as e:
        run.record('RUNNER-'+args.phase,'FAIL','All guards and expected outcomes hold',type(e).__name__+': '+str(e));raise
    print('RUN_DIR='+str(run.folder),flush=True)
