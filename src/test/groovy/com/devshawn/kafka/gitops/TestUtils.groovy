package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import picocli.CommandLine

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.ExecutionException

import static java.util.concurrent.TimeUnit.SECONDS

class TestUtils {
    private static final AclBinding SEED_ACL = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, 'test-topic', PatternType.LITERAL),
            new AccessControlEntry('User:test', '*', AclOperation.READ, AclPermissionType.ALLOW))
    private static final Map SEED_TOPICS = [
            'delete-topic': [partitions: 1, replication: 1, configs: [:]],
            'test-topic': [partitions: 1, replication: 1, configs: [:]],
            'topic-with-configs-1': [partitions: 3, replication: 1, configs: ['cleanup.policy': 'compact', 'segment.bytes': '100000']],
            'topic-with-configs-2': [partitions: 6, replication: 1, configs: ['retention.ms': '60000']]
    ]

    static String getResourceFileContent(String name) {
        normalize(new File(getResourceFilePath(name)).getText('UTF-8'))
    }

    static String getResourceFilePath(String name) {
        Paths.get(TestUtils.classLoader.getResource(name).toURI()).toString()
    }

    static String normalize(String text) {
        text.replace('\r\n', '\n').replace('\r', '\n')
    }

    static Map runCli(String... args) {
        def output = new ByteArrayOutputStream()
        def errors = new ByteArrayOutputStream()
        def oldOut = System.out
        def oldErr = System.err
        int code
        try (def out = new PrintStream(output, true, StandardCharsets.UTF_8);
             def err = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            System.setOut(out)
            System.setErr(err)
            def command = new CommandLine(new MainCommand())
            command.out = new PrintWriter(out, true)
            command.err = new PrintWriter(err, true)
            code = command.execute(args)
        } finally {
            System.setOut(oldOut)
            System.setErr(oldErr)
        }
        [code: code, out: normalize(output.toString(StandardCharsets.UTF_8)), err: normalize(errors.toString(StandardCharsets.UTF_8))]
    }

    static Admin openFixtureAdmin() {
        String expected = System.getenv('GITOPS_INTEGRATION_CLUSTER_ID')
        assert expected && System.getenv('KAFKA_BOOTSTRAP_SERVERS'): 'Set an explicit bootstrap and cluster ID for the disposable integration fixture.'
        Admin admin = Admin.create(KafkaGitopsConfigLoader.load().config)
        try {
            assert readResult(admin.describeCluster().clusterId()) == expected: 'Integration fixture cluster ID mismatch.'
            return admin
        } catch (Throwable failure) {
            admin.close(Duration.ZERO)
            throw failure
        }
    }

    static void closeFixtureAdmin(Admin admin) {
        if (admin == null) return
        try {
            cleanUpCluster(admin)
        } finally {
            admin.close(Duration.ZERO)
        }
    }

    static void cleanUpCluster(Admin admin) {
        def topics = readResult(admin.listTopics().names())
        Set<AclBinding> acls = readAcls(admin)
        if (topics) readResult(admin.deleteTopics(topics).all())
        if (acls) readResult(admin.deleteAcls(acls.collect { it.toFilter() }).all())
        awaitState([topics: [] as Set, acls: [] as Set]) {
            [topics: TestUtils.readResult(admin.listTopics().names()), acls: TestUtils.readAcls(admin)]
        }
        awaitDeletedTopics(admin, topics)
    }

    static void seedCluster(Admin admin) {
        def topics = SEED_TOPICS.collect { name, spec ->
            new NewTopic(name, spec.partitions, (short) spec.replication).configs(spec.configs)
        }
        readResult(admin.createTopics(topics).all())
        readResult(admin.createAcls([SEED_ACL]).all())
        assertState(admin, [topics: SEED_TOPICS, acls: [SEED_ACL] as Set])
    }

    static Map snapshot(Admin admin) {
        def names = readResult(admin.listTopics().names())
        Map topics = [:]
        if (names) {
            def descriptions = readResult(admin.describeTopics(names).allTopicNames())
            def configs = readResult(admin.describeConfigs(names.collect { new ConfigResource(ConfigResource.Type.TOPIC, it) }).all())
            topics = descriptions.collectEntries { name, topic ->
                def overridden = configs[new ConfigResource(ConfigResource.Type.TOPIC, name)].entries()
                        .findAll { it.source() == ConfigSource.DYNAMIC_TOPIC_CONFIG }
                        .collectEntries { [(it.name()): it.value()] }
                def factors = topic.partitions().collect { it.replicas().size() }.unique()
                assert factors.size() == 1
                [(name): [partitions: topic.partitions().size(), replication: factors.first(), configs: overridden]]
            }
        }
        [topics: topics, acls: readAcls(admin)]
    }

    static void assertState(Admin admin, Map expected) {
        awaitState(expected) { TestUtils.snapshot(admin) }
    }

    static void awaitDeletedTopics(Admin admin, Set<String> deleted) {
        Set<String> pending = new LinkedHashSet<>(deleted)
        long deadline = System.nanoTime() + SECONDS.toNanos(20)
        while (!pending.isEmpty()) {
            // A topic marked for deletion drops out of listTopics before its name is reusable.
            def requests = pending.collect { new NewTopic(it, 1, (short) 1) }
            def results = admin.createTopics(requests, new CreateTopicsOptions().validateOnly(true)).values()
            pending = pending.findAll { name ->
                try {
                    TestUtils.readResult(results[name])
                    return false
                } catch (ExecutionException failure) {
                    if (failure.cause instanceof TopicExistsException) return true
                    throw failure
                }
            }
            if (!pending.isEmpty()) {
                assert System.nanoTime() < deadline: "Topic deletion did not release these names: ${pending}"
                Thread.sleep(100)
            }
        }
    }

    private static void awaitState(Map expected, Closure<Map> read) {
        long deadline = System.nanoTime() + SECONDS.toNanos(20)
        Map actual = read.call()
        while (actual != expected && System.nanoTime() < deadline) {
            Thread.sleep(100)
            actual = read.call()
        }
        assert actual == expected
    }

    private static Set<AclBinding> readAcls(Admin admin) {
        readResult(admin.describeAcls(AclBindingFilter.ANY).values()) as Set
    }

    private static <T> T readResult(KafkaFuture<T> future) {
        future.get(10, SECONDS)
    }
}
