package com.devshawn.kafka.gitops.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.core.read.ListAppender
import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.MissingConfigurationException
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.exception.WritePlanOutputException
import com.devshawn.kafka.gitops.manager.PlanManager
import com.devshawn.kafka.gitops.util.LogUtil
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.resource.*
import org.junit.Rule
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import com.devshawn.kafka.gitops.MainCommand
import picocli.CommandLine

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE

/** Failure injection only: no concrete Kafka AdminClient can be constructed by these tests. */
class KafkaDiagnosticsAuditSpec extends Specification {
    @TempDir File directory
    @Rule EnvironmentVariables environment = new EnvironmentVariables()
    Logger logger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    def oldLevel = logger.level
    ListAppender appender = new ListAppender()

    void setup() {
        System.getenv().keySet().findAll { it.startsWith('KAFKA_') }.each { environment.clear(it) }
        logger.level = Level.INFO
        appender.start(); logger.addAppender(appender)
    }
    void cleanup() { logger.detachAppender(appender); appender.stop(); logger.level = oldLevel; Thread.interrupted() }

    @Unroll
    void 'legacy #operation #mode failure does not expose external exception text and closes Admin'() {
        given:
        def fake = new DiagnosticAdmin(mode:mode)
        def service = service(fake)

        when:
        invoke(service,operation)

        then:
        def error = thrown(KafkaExecutionException)
        !error.message.contains('private-')
        !error.exceptionMessage.contains('private-')
        !render(error).contains('private-')
        fake.closes == 1

        where:
        [operation,mode] << ['getAcls','createAcl','deleteAcl','createTopic','deleteTopic','updateTopicConfig','getTopics','describeConfigsForTopics']
                .collectMany { name -> ['future','runtime','close'].collect { [name,it] } }
    }

    void 'legacy future interruption restores the interrupt status and closes its session'() {
        given:
        def fake = new DiagnosticAdmin(mode:'interrupt')

        when:
        service(fake).getAcls()

        then:
        def error = thrown(KafkaExecutionException)
        Thread.currentThread().isInterrupted()
        !render(error).contains('private-')
        fake.closes == 1
    }

    void 'legacy Admin factory failure is sanitized before CLI formatting'() {
        given:
        def service = new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override protected Admin buildAdminClient() { throw new IllegalStateException('private-jaas-and-token') }
        }

        when:
        service.getTopics()

        then:
        def error = thrown(KafkaExecutionException)
        !render(error).contains('private-')
    }

    @Unroll
    void 'configuration file failure #kind stops loading without logging paths or raw exceptions'() {
        given:
        File file = new File(directory,'private-config-token.properties')
        if (kind == 'malformed') file.text = 'sasl.jaas.config=private-password\\uZZZZ'

        when:
        KafkaGitopsConfigLoader.load(file)

        then:
        def error = thrown(MissingConfigurationException)
        !error.message.contains('private-')
        appender.list.every { event -> !event.formattedMessage.contains('private-') && event.throwableProxy == null }

        where:
        kind << ['absent','malformed']
    }

    @Unroll
    void 'invalid state #kind cannot echo confidential input through parser diagnostics'() {
        given:
        File file = new File(directory,'state.yaml')
        file.text = content

        when:
        new ParserService(file).parseStateFile()

        then:
        def error = thrown(ValidationException)
        !error.message.contains('private-')
        !error.message.contains('LoginModule')

        where:
        kind | content
        'invalid scalar' | 'topics:\n  orders:\n    partitions: private-jaas-password\n    replication: 1\n'
        'invalid sensitive config structure' | 'topics:\n  orders:\n    partitions: 1\n    replication: 1\n    configs:\n      sasl.jaas.config: [private-password]\n'
        'unknown credential field' | 'topics:\n  orders:\n    partitions: 1\n    replication: 1\n    private-token-field: private-secret\n'
        'malformed YAML' | 'topics:\n  orders:\n    configs: {sasl.jaas.config: "LoginModule required password=private-secret\n'
        'missing required after credential input' | 'topics:\n  private-token-topic:\n    configs:\n      sasl.jaas.config: private-secret\n'
    }

    void 'JSON export filesystem errors do not echo a confidential destination or a raw cause'() {
        given:
        File file = new File(directory,'output.json') {
            @Override boolean createNewFile() { throw new IOException('private-output-token') }
        }
        def manager = new PlanManager(options(file),null,mapper())

        when:
        manager.writePlanToFile(plan([config('retention.ms',UPDATE,'2000','1000')]))

        then:
        def error = thrown(WritePlanOutputException)
        !error.message.contains('private-')
        !file.exists()
    }

    @Unroll
    void 'sanitized parser diagnostics retain the error fixture contract for #fixture'() {
        given:
        File input = new File(getClass().getResource('/plans/' + fixture + '.yaml').toURI())
        String expected = getClass().getResource('/plans/' + fixture + '-output.txt').getText('UTF-8').replace('\r\n','\n')

        when:
        new ParserService(input).parseStateFile()

        then:
        def error = thrown(ValidationException)
        'Generating execution plan...\n\n[INVALID] ' + error.message + '\n' == expected

        where:
        fixture << ['invalid-format','invalid-topic','unrecognized-property','invalid-storage-topics']
    }

    @Unroll
    void 'CLI command-config #kind fails locally and safely before any Kafka use'() {
        given:
        File file = new File(directory,'private-command-config.properties')
        if (kind == 'malformed') file.text = 'sasl.jaas.config=private-password\\uZZZZ'
        File state = new File(directory,'state.yaml')
        state.text = 'topics:\n  orders:\n    partitions: 1\n    replication: 1\n'

        when:
        def result = cli(['--verbose','--command-config',file.absolutePath,'--file',state.absolutePath,'validate'])

        then:
        result.code == 2
        !result.text.contains('private-')
        !result.text.contains('Successfully validated')

        where:
        kind << ['absent','malformed']
    }

    void 'CLI loads command-config safe diagnostics while retaining secret values outside logs'() {
        given:
        File file = new File(directory,'connection.properties')
        file.text = 'bootstrap.servers=unit-file.invalid:1\nsasl.jaas.config=private-file-jaas\nssl.key.password=private-file-key\n'
        File state = new File(directory,'state.yaml')
        state.text = 'topics:\n  orders:\n    partitions: 1\n    replication: 1\n'

        when:
        def result = cli(['--verbose','--command-config',file.absolutePath,'--file',state.absolutePath,'validate'])

        then:
        result.code == 0
        appender.list.any { it.formattedMessage.contains('unit-file.invalid:1') }
        !result.text.contains('private-')
        appender.list.every { !it.formattedMessage.contains('private-') }
    }

    private static Map cli(List<String> args) {
        def out = System.out; def err = System.err
        def bytes = new ByteArrayOutputStream()
        def command = new CommandLine(new MainCommand())
        int code
        try {
            def stream = new PrintStream(bytes,true,'UTF-8')
            System.setOut(stream); System.setErr(stream)
            command.out = new PrintWriter(stream,true); command.err = new PrintWriter(stream,true)
            code = command.execute(*args)
        } finally { System.setOut(out); System.setErr(err) }
        [code:code,text:bytes.toString('UTF-8')]
    }

    private static KafkaService service(DiagnosticAdmin fake) {
        new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
    }
    private static void invoke(KafkaService service,String operation) {
        def acl = new AclBinding(new ResourcePattern(ResourceType.TOPIC,'orders',PatternType.LITERAL),
                new AccessControlEntry('User:test','*',AclOperation.READ,AclPermissionType.ALLOW))
        switch(operation) {
            case 'getAcls': service.getAcls(); break
            case 'createAcl': service.createAcl(acl); break
            case 'deleteAcl': service.deleteAcl(acl); break
            case 'createTopic': service.createTopic('orders',details()); break
            case 'deleteTopic': service.deleteTopic('orders'); break
            case 'updateTopicConfig': service.updateTopicConfig([:]); break
            case 'getTopics': service.getTopics(); break
            case 'describeConfigsForTopics': service.describeConfigsForTopics(['orders']); break
            default: throw new AssertionError(operation)
        }
    }
    private static String render(KafkaExecutionException error) {
        def previous = System.out
        def output = new ByteArrayOutputStream()
        try { System.setOut(new PrintStream(output,true,'UTF-8')); LogUtil.printKafkaExecutionError(error,true) }
        finally { System.setOut(previous) }
        output.toString('UTF-8')
    }
}

class DiagnosticAdmin implements InvocationHandler {
    String mode
    int closes
    Admin admin = (Admin) Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],this)

    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if (method.name == 'close') {
            closes++
            if (mode == 'close') throw new IllegalStateException('private-close-password')
            return null
        }
        assert method.name in ['describeAcls','createAcls','deleteAcls','createTopics','deleteTopics','incrementalAlterConfigs','listTopics','describeConfigs']
        if (mode == 'runtime') throw new IllegalStateException('private-server-jaas-password')
        def future = new KafkaFutureImpl()
        if (mode == 'future') future.completeExceptionally(new IllegalStateException('private-server-oauth-token'))
        else future.complete(null)
        switch(method.name) {
            case 'describeAcls':
                if (mode == 'interrupt') return new DescribeAclsResult(new InterruptGetFuture())
                if (mode == 'close') future = org.apache.kafka.common.KafkaFuture.completedFuture([])
                return new DescribeAclsResult(future)
            case 'createAcls': return new CreateAclsResult([(args[0].first()):future])
            case 'deleteAcls':
                if (mode == 'close') future = org.apache.kafka.common.KafkaFuture.completedFuture(new DeleteAclsResult.FilterResults([]))
                return new DeleteAclsResult([(args[0].first()):future])
            case 'createTopics': return new CreateTopicsResult(['orders':future])
            case 'deleteTopics': return DeleteTopicsResult.ofTopicNames(['orders':future])
            case 'incrementalAlterConfigs': return new AlterConfigsResult([(new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,'orders')):future])
            case 'listTopics':
                if (mode == 'close') future = org.apache.kafka.common.KafkaFuture.completedFuture([:])
                return new ListTopicsResult(future)
            case 'describeConfigs': return new DescribeConfigsResult([(args[0].first()):future])
        }
        throw new AssertionError(method.name)
    }
}

class InterruptGetFuture extends KafkaFutureImpl {
    @Override Object get() { throw new InterruptedException('private-interrupted-password') }
    @Override Object get(long timeout, java.util.concurrent.TimeUnit unit) { throw new InterruptedException('private-interrupted-password') }
}
