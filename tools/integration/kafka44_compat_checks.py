"""Additional compatibility negatives on the already completed, new Kafka 4.4 RF namespace."""
import argparse
from kafka44_elr_rf import ElrRun, PROPS, read

def run_extra(run):
    assert any(r['id']=='RF32' and r['status']=='PASS' for r in run.manifest['results'])
    before=run.gate('COMPAT-before')
    y=run.yaml('COMPAT-PARTITION','rf',partitions=1,rf=2)
    _,r,args=run.plan('COMPAT-PARTITION-plan',y,expected=2,guarded=True)
    run.record('COMPAT-PARTITION','PASS','Reject partition decrease','2->1 rejected; no JSON; zero guarded write attempts',args,2)
    y=run.yaml('COMPAT-CONFIG','rf',rf=2,configs={'max.message.bytes':'-1'})
    p,_,_=run.plan('COMPAT-CONFIG-plan',y,guarded=True)
    b,a,r,args=run.apply('COMPAT-CONFIG',y,p,expected=2)
    assert b['topics']==a['topics']==before['topics']
    run.record('COMPAT-CONFIG','PASS','Reject invalid config','Broker rejected intended invalid config request; exit2; metadata unchanged',args,2,b,a)
    unavailable=run.folder/'compat-unavailable.properties'
    unavailable.write_text('bootstrap.servers=127.0.0.1:29999\nsecurity.protocol=PLAINTEXT\nrequest.timeout.ms=500\ndefault.api.timeout.ms=1000\n')
    y=run.yaml('COMPAT-BOOTSTRAP','rf',rf=2);args=run.args(y,'plan');args[args.index(PROPS)]=unavailable
    r=run.command('COMPAT-BOOTSTRAP',args,2);assert 'TIMEOUT' in r.stdout
    run.record('COMPAT-BOOTSTRAP','PASS','Bounded unreachable loopback bootstrap','TIMEOUT, exit2; no external endpoint',args,2)
    run.record('COMPAT-AUTH','NOT_TESTED','Authorization error if applicable','PLAINTEXT lab has no authorizer; ACL semantics unchanged')
    logs='\n'.join(p.read_text(encoding='utf-8') for p in run.folder.glob('*.log'))
    assert not any(s in logs for s in ['sasl.jaas.config=','-----BEGIN PRIVATE KEY-----','ignored-environment-client'])
    run.record('COMPAT-SECRETS','PASS','No secrets in logs','No credential markers; loopback/synthetic properties only',exit_code=0)
    run.data_check('COMPAT-FINAL');run.verify()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',required=True);parser.add_argument('--allow-writes',action='store_true')
    args=parser.parse_args()
    if not args.allow_writes:parser.error('Explicit --allow-writes is required for the intended invalid config request')
    run=ElrRun(args.run_dir)
    try:run_extra(run)
    except Exception as e:
        run.record('RUNNER-COMPAT','FAIL','Compatibility guards hold',type(e).__name__+': '+str(e));raise
