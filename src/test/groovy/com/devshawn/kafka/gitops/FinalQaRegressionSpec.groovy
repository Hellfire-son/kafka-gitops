package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.service.*
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.junit.Rule
import org.junit.contrib.java.lang.system.EnvironmentVariables
import picocli.CommandLine
import spock.lang.Specification
import spock.lang.TempDir

import java.lang.reflect.Method

/** Real CLI/planner/reader with strict in-memory Admin. No network clients are constructible. */
class FinalQaRegressionSpec extends Specification {
    @TempDir File directory
    @Rule EnvironmentVariables environment=new EnvironmentVariables()
    void setup() { System.getenv().keySet().findAll { it.startsWith('KAFKA_') }.each { environment.clear(it) } }

    File state(String content) { def f=new File(directory,'state.yaml'); f.text=content; f }

    Map cli(File state,List<String> args,KafkaService kafka=new TestKafkaService()) {
        def bytes=new ByteArrayOutputStream()
        def writer=new PrintWriter(bytes,true)
        def previous=System.out
        def command=new CommandLine(new MainCommand({ cfg,parser -> new StateManager(cfg,parser,kafka) } as MainCommand.StateManagerFactory))
        command.setOut(writer); command.setErr(writer)
        command.subcommands.values().each { it.setOut(writer); it.setErr(writer) }
        int code
        try { System.setOut(new PrintStream(bytes,true,'UTF-8')); code=command.execute(*(['-f',state.absolutePath]+args)) }
        finally { writer.flush(); System.setOut(previous) }
        [code:code,text:bytes.toString('UTF-8')]
    }

    void 'null YAML root returns a safe validation failure: #input'() {
        when:
        def result=cli(state(input),['plan'])

        then:
        result.code==2
        result.text.contains('state file')
        !result.text.contains('NullPointerException')
        !result.text.contains('SUCCESS')

        where:
        input << ['null\n','~\n']
    }

    void 'invalid explicit RF is rejected before planning or exporting: #rf'() {
        given:
        def kafka=new TestKafkaService(topics:[])
        File out=new File(directory,'invalid-rf.json')

        when:
        def result=cli(state("topics:\n  fresh:\n    partitions: 1\n    replication: ${rf}\n"),['plan','-o',out.absolutePath],kafka)

        then:
        result.code==2
        !out.exists()
        kafka.createdTopics.empty && kafka.operations.empty

        where:
        rf << [0,-1,32768]
    }

    void 'invalid RF in old-schema ADD JSON blocks every write including an earlier valid ADD: #rf'() {
        given:
        def kafka=new TestKafkaService(topics:[])
        File plan=new File(directory,'invalid-add.json')
        plan.text="""{"topicPlans":[{"name":"first","action":"ADD","topicDetails":{"partitions":1,"replication":1}},
            {"name":"bad","action":"ADD","topicDetails":{"partitions":1,"replication":${rf}}}],"aclPlans":[]}"""

        when:
        def result=cli(state('topics: {}\n'),['apply','-p',plan.absolutePath],kafka)

        then:
        result.code==2
        kafka.createdTopics.empty && kafka.operations.empty
        !result.text.contains('Apply complete!')

        where:
        rf << [0,-1,32768]
    }

    void 'invalid default RF is rejected while topic override and maximum RF remain valid'() {
        expect:
        cli(state('settings:\n  topics:\n    defaults:\n      replication: 32768\ntopics:\n  test-topic:\n    partitions: 1\n'),['plan']).code==2

        and:
        cli(state('settings:\n  topics:\n    defaults:\n      replication: 2\ntopics:\n  fresh:\n    partitions: 1\n    replication: 32767\n'),['plan'],new TestKafkaService(topics:[])).code==0
    }

    void 'blocked topic config read retains desired config values without revealing confidential values'() {
        given:
        def kafka=new TestKafkaService() {
            @Override Map describeConfigsForTopics(List names) { throw new KafkaExecutionException('Read configs','Metadata unavailable',KafkaExecutionException.Code.AUTHORIZATION_FAILED) }
        }
        File out=new File(directory,'blocked.json')

        when:
        def result=cli(state('topics:\n  test-topic:\n    partitions: 1\n    replication: 1\n    configs:\n      max.message.bytes: 10485880\n      sasl.jaas.config: private-desired-secret\n'),['plan','-o',out.absolutePath],kafka)

        then:
        result.code==2
        result.text.contains('Config max.message.bytes: UNKNOWN -> 10485880')
        result.text.contains('sasl.jaas.config: [redacted] -> [redacted]')
        !result.text.contains('private-desired-secret')
        result.text.indexOf('10485880')<result.text.indexOf('[BLOCKED]')
        !out.exists()
        kafka.createdTopics.empty && kafka.operations.empty
    }

    void 'all commands support a local help option without invoking their command: #name'() {
        given:
        def command=new CommandLine(new MainCommand())

        when:
        def parsed=command.parseArgs(name,'--help')

        then:
        parsed.subcommand().usageHelpRequested

        where:
        name << ['plan','apply','validate','account']
    }

    void 'invalid root CLI input cannot return a successful automation exit code'() {
        expect:
        cli(state('topics: {}\n'),['--qa-unknown-option']).code==2
    }

    void 'full CLI plan uses real read APIs only and closes every Admin session: #flags'() {
        given:
        def fake=new QaReadOnlyAdmin()
        int opens=0
        def service=new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { opens++; fake.admin }
        }
        File out=new File(directory,'read-only.json')

        when:
        def result=cli(state('topics:\n  acl-test:\n    partitions: 4\n    replication: 2\n    configs:\n      max.message.bytes: 10485880\n'),['plan','-o',out.absolutePath]+flags,service)

        then:
        result.code==0
        result.text.contains('Partitions: 4 -> 4')
        result.text.contains('Replication: [1] -> 2')
        result.text.contains('unit.invalid:9092') || result.text.contains('Connecting')
        out.exists()
        fake.writeCalls==0
        fake.calls.count('close')==opens
        opens>=2

        where:
        flags << [[],['--manage-replication'],['--no-manage-replication']]
    }
}

class QaReadOnlyAdmin extends Task2ReadOnlyAdmin {
    int writeCalls
    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if(method.name==~ /^(create|delete|alter|incrementalAlter|update).*/) {
            writeCalls++
            throw new AssertionError('Plan attempted a mutating Admin API: '+method.name)
        }
        if(method.name=='listTopics') {
            calls.add(method.name)
            return new ListTopicsResult(KafkaFuture.completedFuture(state.topics().collectEntries { name,t -> [(name):new TopicListing(name,org.apache.kafka.common.Uuid.fromString(t.topicId()),false)] }))
        }
        if(method.name=='describeAcls') { calls.add(method.name); return new DescribeAclsResult(KafkaFuture.completedFuture([])) }
        if(method.name=='describeConfigs' && args.length==1) args=[args[0],new DescribeConfigsOptions().timeoutMs(2000)] as Object[]
        super.invoke(proxy,method,args)
    }
}
