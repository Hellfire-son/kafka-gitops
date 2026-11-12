package com.devshawn.kafka.gitops.service

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.core.read.ListAppender
import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.security.JaasConfig
import org.junit.Rule
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.util.function.Supplier

import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.UPDATE

class FinalQaResourceSpec extends Specification {
    @Rule EnvironmentVariables environment=new EnvironmentVariables()
    void setup() { System.getenv().keySet().findAll { it.startsWith('KAFKA_') }.each { environment.clear(it) } }
    void cleanup() { Thread.interrupted() }

    void 'RF snapshot interruption survives a close implementation that clears the flag'() {
        given:
        int closes=0
        def interrupted=new KafkaFutureImpl<FeatureMetadata>() {
            @Override FeatureMetadata get(long timeout,TimeUnit unit) throws InterruptedException { throw new InterruptedException('private-interrupted') }
        }
        Admin admin=Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],{ p,m,args ->
            if(m.name=='describeFeatures') return new DescribeFeaturesResult(interrupted)
            if(m.name=='close') { closes++; Thread.interrupted(); return null }
            throw new AssertionError(m.name)
        } as java.lang.reflect.InvocationHandler) as Admin
        def kafka=new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override protected Admin buildAdminClient() { admin }
        }

        when:
        kafka.readReplicationSnapshot(['orders'] as Set)

        then:
        def error=thrown(KafkaExecutionException)
        error.code==KafkaExecutionException.Code.INTERRUPTED
        Thread.currentThread().isInterrupted()
        closes==1
    }

    void 'RF application interruption survives session close without any execution or writes'() {
        given:
        int closes=0
        int executions=0
        Admin admin=Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],{ p,m,args ->
            assert m.name=='close'
            closes++; Thread.interrupted(); return null
        } as java.lang.reflect.InvocationHandler) as Admin
        def time=new AdminReassignmentExecutor.TimeSource() {
            long nanoTime() { System.nanoTime() }
            void pause(long nanos) { throw new AssertionError('No waiting allowed') }
        }
        def application=new AdminReplicationApplication({ admin } as Supplier,
                new AdminReassignmentExecutor.Settings(1,Duration.ofSeconds(1),Duration.ofMillis(1)),time,
                { session,names,timeout -> throw new InterruptedException('private-preflight') } as AdminReassignmentExecutor.SnapshotReader,
                Duration.ofSeconds(2),{ engine,request -> executions++; throw new AssertionError('No execution allowed') } as BiFunction)
        def topic=new TopicPlan.Builder().setName('orders').setAction(UPDATE).setReplicationPlan(fixture().plan).build()

        when:
        application.apply([topic],permissions())

        then:
        thrown(ReplicationPreflightException)
        Thread.currentThread().isInterrupted()
        closes==1
        executions==0
    }

    void 'generated environment JAAS preserves backslashes and quotes after Kafka parses it: #username'() {
        given:
        environment.set('KAFKA_SECURITY_PROTOCOL','SASL_SSL')
        environment.set('KAFKA_SASL_MECHANISM','PLAIN')
        environment.set('KAFKA_SASL_JAAS_USERNAME',username)
        environment.set('KAFKA_SASL_JAAS_PASSWORD',password)

        when:
        String jaas=KafkaGitopsConfigLoader.load().config['sasl.jaas.config']
        def parsed=new JaasConfig('KafkaClient',jaas).getAppConfigurationEntry('KafkaClient')[0].options

        then:
        parsed['username']==username
        parsed['password']==password

        where:
        username | password
        'DOMAIN\\user' | 'private\\password'
        'private\\"user' | 'private\\"password\\'
    }

    void 'malformed bootstrap credentials are redacted even in verbose configuration logging'() {
        given:
        environment.set('KAFKA_BOOTSTRAP_SERVERS','private-user:private-password@unit.invalid:9092')
        Logger logger=(Logger)LoggerFactory.getLogger(KafkaGitopsConfigLoader)
        def before=logger.level
        def capture=new ListAppender()
        capture.start(); logger.level=Level.INFO; logger.addAppender(capture)

        when:
        KafkaGitopsConfigLoader.load()

        then:
        capture.list.every { !it.formattedMessage.contains('private-user') && !it.formattedMessage.contains('private-password') }

        cleanup:
        logger.detachAppender(capture); capture.stop(); logger.level=before
    }

    void 'connection target diagnostics accept endpoints and redact malformed data: #configured'() {
        expect:
        com.devshawn.kafka.gitops.util.KafkaDiagnostics.bootstrap(configured)==expected

        where:
        configured | expected
        null | 'UNKNOWN (not configured)'
        '' | 'UNKNOWN (not configured)'
        'unit.invalid:9092' | 'unit.invalid:9092'
        '[::1]:9092' | '[::1]:9092'
        ['one.invalid:9092','two.invalid:9093'] | 'one.invalid:9092, two.invalid:9093'
        'private-user:private-password@unit.invalid:9092' | '[redacted invalid endpoint]'
        'unit.invalid:0' | '[redacted invalid endpoint]'
        'unit.invalid:9092\nprivate-token' | '[redacted invalid endpoint]'
    }
}
