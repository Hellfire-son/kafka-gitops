package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ListTopicsResult
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.errors.*
import org.junit.Rule
import org.junit.contrib.java.lang.system.EnvironmentVariables
import spock.lang.Specification
import spock.lang.TempDir

import java.lang.reflect.Proxy
import java.time.Duration

class KafkaConnectionTask2Spec extends Specification {
    @Rule EnvironmentVariables environment=new EnvironmentVariables()
    @TempDir File directory
    void setup() { System.getenv().keySet().findAll { it.startsWith('KAFKA_') }.each { environment.clear(it) } }
    void cleanup() { Thread.interrupted() }

    void 'missing bootstrap never gets a localhost fallback or opens an Admin'() {
        given:
        def config=KafkaGitopsConfigLoader.load()

        when:
        new KafkaService(config).getTopics()

        then:
        !config.config.containsKey('bootstrap.servers')
        def error=thrown(KafkaExecutionException)
        error.code==KafkaExecutionException.Code.BOOTSTRAP_MISSING
        error.exceptionMessage.contains('UNKNOWN')
        !error.exceptionMessage.contains('localhost')
    }

    void 'file properties beat conflicting environment for #protocol / #mechanism without reading certificates or keytabs'() {
        given:
        environment.set('KAFKA_BOOTSTRAP_SERVERS','environment.invalid:1')
        environment.set('KAFKA_SECURITY_PROTOCOL','SASL_SSL')
        environment.set('KAFKA_SASL_JAAS_CONFIG','private-env-jaas')
        environment.set('KAFKA_CLIENT_ID','environment-client')
        File file=new File(directory,'client.properties')
        file.text="bootstrap.servers=localhost:9092\nclient.id=file-client\nsecurity.protocol=${protocol}\nsasl.mechanism=${mechanism}\nsasl.jaas.config=private-file-jaas\nssl.truststore.location=synthetic-truststore\nssl.truststore.password=private-trust\nssl.keystore.location=synthetic-keystore\nssl.keystore.password=private-key\nrequest.timeout.ms=1000\ndefault.api.timeout.ms=2000\n"

        when:
        def config=KafkaGitopsConfigLoader.load(file).config

        then:
        config['bootstrap.servers']=='localhost:9092'
        config['client.id']=='file-client'
        config['security.protocol']==protocol
        config['sasl.mechanism']==mechanism
        config['sasl.jaas.config']=='private-file-jaas'
        config['request.timeout.ms']=='1000'
        config['default.api.timeout.ms']=='2000'
        config['ssl.truststore.password']=='private-trust'

        where:
        protocol | mechanism
        'PLAINTEXT' | 'PLAIN'
        'SSL' | 'GSSAPI'
        'SASL_PLAINTEXT' | 'PLAIN'
        'SASL_SSL' | 'PLAIN'
        'SASL_SSL' | 'GSSAPI'
    }

    void 'explicit plaintext does not require SASL shortcut credentials'() {
        given:
        environment.set('KAFKA_SASL_JAAS_USERNAME','private-unused-user')
        File file=new File(directory,'plain.properties')
        file.text='bootstrap.servers=unit.invalid:1\nsecurity.protocol=PLAINTEXT\n'

        expect:
        KafkaGitopsConfigLoader.load(file).config['security.protocol']=='PLAINTEXT'
    }

    void 'environment remains the source when no command config was specified'() {
        given:
        environment.set('KAFKA_BOOTSTRAP_SERVERS','environment.invalid:1')
        environment.set('KAFKA_CLIENT_ID','environment-client')

        expect:
        KafkaGitopsConfigLoader.load().config['bootstrap.servers']=='environment.invalid:1'
        KafkaGitopsConfigLoader.load().config['client.id']=='environment-client'
    }

    void 'GSSAPI using external JAAS is not replaced by unrelated environment credential shortcuts'() {
        given:
        environment.set('KAFKA_SASL_JAAS_USERNAME','private-unused-user')
        environment.set('KAFKA_SASL_JAAS_PASSWORD','private-unused-password')
        File file=new File(directory,'kerberos.properties')
        file.text='bootstrap.servers=unit.invalid:1\nsecurity.protocol=SASL_SSL\nsasl.mechanism=GSSAPI\n'

        when:
        def config=KafkaGitopsConfigLoader.load(file).config

        then:
        config['sasl.mechanism']=='GSSAPI'
        !config.containsKey('sasl.jaas.config')
    }

    void 'failure type #failure is classified without its confidential message: #expected'() {
        given:
        def future=new KafkaFutureImpl()
        future.completeExceptionally(failure)
        int closes=0
        Admin admin=Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],{ proxy,method,args ->
            if(method.name=='listTopics') return new ListTopicsResult(future)
            if(method.name=='close') { assert args[0]==Duration.ZERO; closes++; return null }
            throw new AssertionError(method.name)
        } as java.lang.reflect.InvocationHandler) as Admin
        def service=new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override protected Admin buildAdminClient() { admin }
        }

        when:
        service.getTopics()

        then:
        def error=thrown(KafkaExecutionException)
        error.code.name()==expected
        !error.exceptionMessage.contains('private-')
        error.cause==null
        closes==1

        where:
        failure | expected
        new java.net.UnknownHostException('private-dns') | 'DNS_FAILURE'
        new java.net.ConnectException('private-refused') | 'CONNECTION_REFUSED'
        new SaslAuthenticationException('private-password') | 'AUTHENTICATION_FAILED'
        new SslAuthenticationException('private-cert') | 'SSL_HANDSHAKE_FAILED'
        new javax.net.ssl.SSLHandshakeException('private-handshake') | 'SSL_HANDSHAKE_FAILED'
        new ClusterAuthorizationException('private-acl') | 'AUTHORIZATION_FAILED'
        new org.apache.kafka.common.errors.TimeoutException('private-timeout') | 'TIMEOUT'
        new InterruptedException('private-interrupt') | 'INTERRUPTED'
        new NetworkException('private-unavailable') | 'KAFKA_UNAVAILABLE'
    }

    void 'uncompleted legacy API wait has a deadline and closes promptly'() {
        given:
        def future=new KafkaFutureImpl()
        int closes=0
        Admin admin=Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],{ proxy,method,args ->
            if(method.name=='listTopics') return new ListTopicsResult(future)
            if(method.name=='close') { closes++; return null }
            throw new AssertionError(method.name)
        } as java.lang.reflect.InvocationHandler) as Admin
        def service=new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',30).build()) {
            @Override protected Admin buildAdminClient() { admin }
        }
        long started=System.nanoTime()

        when:
        service.getTopics()

        then:
        def error=thrown(KafkaExecutionException)
        error.code==KafkaExecutionException.Code.TIMEOUT
        System.nanoTime()-started < java.util.concurrent.TimeUnit.SECONDS.toNanos(2)
        closes==1
    }
}
