package com.devshawn.kafka.gitops.config

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.devshawn.kafka.gitops.StateManager
import com.devshawn.kafka.gitops.service.ParserService
import org.junit.Rule
import org.junit.contrib.java.lang.system.EnvironmentVariables
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

@Unroll
class KafkaGitopsConfigLoggingSpec extends Specification {

    @Rule
    EnvironmentVariables environmentVariables = new EnvironmentVariables()

    @TempDir
    File temporaryDirectory

    private Logger rootLogger
    private Logger kafkaLogger
    private Level previousRootLevel
    private Level previousKafkaLevel
    private ListAppender<ILoggingEvent> logAppender

    void setup() {
        System.getenv().keySet().findAll { it.startsWith("KAFKA_") }.each { environmentVariables.clear(it) }
        rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        kafkaLogger = (Logger) LoggerFactory.getLogger("org.apache.kafka")
        previousRootLevel = rootLogger.level
        previousKafkaLevel = kafkaLogger.level
        rootLogger.setLevel(Level.INFO)
        logAppender = new ListAppender<>()
        logAppender.start()
        rootLogger.addAppender(logAppender)
    }

    void cleanup() {
        rootLogger.detachAppender(logAppender)
        logAppender.stop()
        rootLogger.setLevel(previousRootLevel)
        kafkaLogger.setLevel(previousKafkaLevel)
    }

    void 'omits the entire #format JAAS configuration from INFO logs'() {
        given:
        environmentVariables.set("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        environmentVariables.set("KAFKA_SASL_MECHANISM", mechanism)
        environmentVariables.set("KAFKA_SASL_JAAS_CONFIG", jaas)

        when:
        KafkaGitopsConfig config = KafkaGitopsConfigLoader.load()

        then:
        config.config['sasl.jaas.config'] == jaas
        loggedConfiguration() == [
                'bootstrap.servers': 'localhost:9092',
                'client.id': 'kafka-gitops',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': mechanism
        ]
        !loggedMessages().contains('sasl.jaas.config')
        !loggedMessages().contains('private-jaas')
        !loggedMessages().contains('LoginModule')

        where:
        format               | mechanism       | jaas
        'PLAIN'              | 'PLAIN'         | 'org.apache.kafka.common.security.plain.PlainLoginModule required username="private-jaas-user" password="private-jaas-password";'
        'spaced PLAIN'       | 'PLAIN'         | 'org.apache.kafka.common.security.plain.PlainLoginModule required username = "private-jaas-user" password = "private-jaas-password" debug=true;'
        'single quoted PLAIN'| 'PLAIN'         | "org.apache.kafka.common.security.plain.PlainLoginModule required username='private-jaas-user' password='private-jaas-password';"
        'SCRAM-SHA-256'      | 'SCRAM-SHA-256' | 'org.apache.kafka.common.security.scram.ScramLoginModule required username="private-jaas-user" password="private-jaas-\\\"password";'
        'multiline SCRAM'    | 'SCRAM-SHA-512' | 'org.apache.kafka.common.security.scram.ScramLoginModule required\n username="private-jaas-user"\n password="private-jaas-password"\n;'
        'Kerberos keytab'    | 'GSSAPI'        | 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/private-jaas/client.keytab" principal="private-jaas-user@EXAMPLE.COM" storeKey=true;'
        'Kerberos cache'     | 'GSSAPI'        | 'com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true ticketCache="/private-jaas/krb5cc" principal="private-jaas-user@EXAMPLE.COM";'
        'OAUTHBEARER'        | 'OAUTHBEARER'   | 'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="private-jaas-client" clientSecret="private-jaas-secret" accessToken="private-jaas-token";'
        'multiple modules'   | 'PLAIN'         | 'example.FirstLoginModule required password="private-jaas-first"; example.SecondLoginModule required password="private-jaas-second";'
    }

    void 'omits SSL, OAuth and unknown environment settings without changing their values'() {
        given:
        Map<String, String> settings = [
                'ssl.keystore.password': 'private-keystore-password',
                'ssl.truststore.password': 'private-truststore-password',
                'ssl.key.password': 'private-key-password',
                'ssl.keystore.key': '-----BEGIN PRIVATE KEY-----\nprivate-pem-key\n-----END PRIVATE KEY-----',
                'sasl.oauthbearer.client.credentials.client.secret': 'private-oauth-client-secret',
                'sasl.oauthbearer.client.secret': 'private-other-oauth-secret',
                'sasl.oauthbearer.token.endpoint.url': 'https://identity.invalid/token?credential=private-url-secret',
                'sasl.login.callback.handler.class': 'private.callback.Handler',
                'custom.api.token': 'private-api-token',
                'custom.credential': 'private-credential',
                'custom.option': 'private-unknown-value'
        ]
        settings.each { key, value ->
            environmentVariables.set('KAFKA_' + key.replace('.', '_').toUpperCase(Locale.ROOT), value)
        }

        when:
        KafkaGitopsConfig config = KafkaGitopsConfigLoader.load()

        then:
        settings.every { key, value -> config.config[key] == value }
        loggedConfiguration() == ['bootstrap.servers': 'localhost:9092', 'client.id': 'kafka-gitops']
        settings.keySet().every { !loggedMessages().contains(it) }
        !loggedMessages().contains('private-')
    }

    void 'logs only safe effective settings from a properties file and environment overrides'() {
        given:
        Map<String, String> settings = [
                'bootstrap.servers': 'file-broker:9093',
                'client.id': 'file-client',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'GSSAPI',
                'sasl.jaas.config': 'com.sun.security.auth.module.Krb5LoginModule required\n keyTab="/private-file/keytab" principal="private-file@EXAMPLE.COM";',
                'ssl.keystore.password': 'private-file-keystore',
                'ssl.truststore.password': 'private-file-truststore',
                'ssl.key.password': 'private-file-key',
                'sasl.oauthbearer.client.credentials.client.secret': 'private-file-oauth',
                'Unrecognized.MixedCase.Setting': 'private-file-unknown'
        ]
        Properties properties = new Properties()
        properties.putAll(settings)
        File configFile = new File(temporaryDirectory, 'connection.properties')
        configFile.withOutputStream { properties.store(it, null) }
        environmentVariables.set('KAFKA_BOOTSTRAP_SERVERS', 'environment-broker:9094')

        when:
        KafkaGitopsConfig config = KafkaGitopsConfigLoader.load(configFile)

        then:
        config.config == settings + ['bootstrap.servers': 'environment-broker:9094']
        loggedConfiguration() == [
                'bootstrap.servers': 'environment-broker:9094',
                'client.id': 'file-client',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'GSSAPI'
        ]
        !loggedMessages().contains('private-file')
        !loggedMessages().contains('file-broker:9093')
        !loggedMessages().contains('Unrecognized.MixedCase.Setting')
    }

    void 'preserves generated #mechanism credentials while omitting them from logs'() {
        given:
        environmentVariables.set('KAFKA_SASL_MECHANISM', mechanism)
        environmentVariables.set('KAFKA_SASL_JAAS_USERNAME', 'private-helper-user')
        environmentVariables.set('KAFKA_SASL_JAAS_PASSWORD', 'private-helper-password')

        when:
        KafkaGitopsConfig config = KafkaGitopsConfigLoader.load()

        then:
        config.config['sasl.jaas.config'] == loginModule + ' required username="private-helper-user" password="private-helper-password";'
        loggedConfiguration()['sasl.mechanism'] == mechanism
        !loggedConfiguration().containsKey('sasl.jaas.config')
        !loggedMessages().contains('private-helper')

        where:
        mechanism       | loginModule
        'PLAIN'         | 'org.apache.kafka.common.security.plain.PlainLoginModule'
        'SCRAM-SHA-256' | 'org.apache.kafka.common.security.scram.ScramLoginModule'
        'SCRAM-SHA-512' | 'org.apache.kafka.common.security.scram.ScramLoginModule'
    }

    void 'StateManager verbose=#verbose keeps #mechanism secrets out of application logs'() {
        given:
        environmentVariables.set('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
        environmentVariables.set('KAFKA_SASL_MECHANISM', mechanism)
        environmentVariables.set('KAFKA_SASL_JAAS_CONFIG', 'private-verbose-jaas')
        environmentVariables.set('KAFKA_SSL_KEY_PASSWORD', 'private-verbose-key')
        environmentVariables.set('KAFKA_CUSTOM_OPTION', 'private-verbose-unknown')
        File stateFile = new File(temporaryDirectory, 'unused-state.yaml')
        ManagerConfig managerConfig = new ManagerConfig.Builder()
                .setVerboseRequested(verbose)
                .setDeleteDisabled(false)
                .setIncludeUnchangedEnabled(false)
                .setSkipAclsDisabled(false)
                .setSkipTopicsDisabled(false)
                .setStateFile(stateFile)
                .build()

        when:
        // Construction configures logging and loads credentials, but never creates an AdminClient.
        new StateManager(managerConfig, new ParserService(stateFile))

        then:
        rootLogger.level == (verbose ? Level.INFO : Level.WARN)
        kafkaLogger.level == (verbose ? Level.WARN : Level.OFF)
        configurationEvents().size() == (verbose ? 1 : 0)
        !loggedMessages().contains('private-verbose')
        !verbose || loggedConfiguration() == [
                'bootstrap.servers': 'localhost:9092',
                'client.id': 'kafka-gitops',
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': mechanism
        ]

        where:
        verbose | mechanism
        true    | 'PLAIN'
        true    | 'SCRAM-SHA-256'
        true    | 'SCRAM-SHA-512'
        true    | 'GSSAPI'
        true    | 'OAUTHBEARER'
        false   | 'PLAIN'
    }

    private List<ILoggingEvent> configurationEvents() {
        logAppender.list.findAll { it.loggerName == KafkaGitopsConfigLoader.name }
    }

    private Map<String, Object> loggedConfiguration() {
        List<ILoggingEvent> events = configurationEvents()
        assert events.size() == 1
        assert events.first().level == Level.INFO
        // Inspect the raw argument as well as rendered messages: the full config must never reach SLF4J.
        events.first().argumentArray[0] as Map<String, Object>
    }

    private String loggedMessages() {
        logAppender.list.collect { it.formattedMessage }.join('\n')
    }
}
