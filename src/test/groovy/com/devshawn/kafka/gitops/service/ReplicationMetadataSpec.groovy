package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.*
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration

import static com.devshawn.kafka.gitops.PlanTestSupport.mapper
import static com.devshawn.kafka.gitops.ReplicationPlanningSupport.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*

class ReplicationMetadataSpec extends Specification {
    ReadOnlyAdmin fake

    void setup() { fake = new ReadOnlyAdmin() }

    private KafkaService service(int timeout = 2000) {
        new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms', timeout)
                .putConfig('sasl.jaas.config', 'private-metadata-jaas').build()) {
            @Override
            protected Admin buildAdminClient() { fake.admin }
        }
    }

    void 'collects read-only snapshot with one Admin session including internal-topic replica load'() {
        when:
        def result = service().readReplicationSnapshot(['orders'] as Set)

        then:
        result.clusterId() == fake.state.clusterId()
        result.topics().orders.topicId() == fake.state.topics().orders.topicId()
        result.topics().orders.partitions()*.replicas() == [[0],[1]]
        result.topics().orders.partitions()*.leader()*.asInt == [0,1]
        result.topics().orders.partitions()*.isr() == [[0] as Set,[1] as Set]
        result.topics().orders.minInSyncReplicas().asInt == 1
        result.brokers().values().every { it.isPreviewCandidate() && it.unfenced() == UNKNOWN }
        result.replicaCounts() == [0:2L,1:2L,2:1L,3:0L]
        result.reassignmentState() == VERIFIED
        result.loadState() == VERIFIED
        fake.calls.count('describeCluster') == 2
        fake.calls.count('describeTopics') == 2
        fake.calls.count('close') == 1
        !result.toString().contains('private-')
        !mapper().writeValueAsString(result).contains('private-')
    }

    @Unroll
    void 'cordoning remains explicit #value sensitive=#sensitive -> #status'() {
        given:
        fake.brokerEntries[1] = value == null ? [] : [entry('cordoned.log.dirs', value, sensitive)]

        when:
        def b = service().readReplicationSnapshot(['orders'] as Set).brokers()[1]

        then:
        b.uncordoned() == status
        b.isPreviewCandidate() == (status == VERIFIED)
        !b.toString().contains('private-')

        where:
        value | sensitive | status
        '' | false | VERIFIED
        '*' | false | UNAVAILABLE
        '/private-cordoned-path' | false | UNAVAILABLE
        null | false | UNKNOWN
        '' | true | UNKNOWN
    }

    @Unroll
    void 'broker read failure #failure preserves UNKNOWN or UNAVAILABLE'() {
        given:
        fake.directoryFutures[1] = failed(failure)

        when:
        def b = service().readReplicationSnapshot(['orders'] as Set).brokers()[1]

        then:
        b.reachable() == reachable
        b.storage() == storage
        !b.isPreviewCandidate()
        fake.calls.count('close') == 1

        where:
        failure | reachable | storage
        new BrokerNotAvailableException('private-failure') | UNAVAILABLE | UNKNOWN
        new org.apache.kafka.common.errors.TimeoutException('private-failure') | UNAVAILABLE | UNKNOWN
        new ClusterAuthorizationException('private-failure') | UNKNOWN | UNKNOWN
        new UnsupportedVersionException('private-failure') | UNKNOWN | UNKNOWN
        new KafkaStorageException('private-failure') | VERIFIED | UNAVAILABLE
    }

    @Unroll
    void 'log directory state #caseName cannot confirm new placement'() {
        given:
        fake.directoryFutures[1] = KafkaFuture.completedFuture(directories)

        when:
        def b = service().readReplicationSnapshot(['orders'] as Set).brokers()[1]

        then:
        b.storage() == storage
        !b.isPreviewCandidate()

        where:
        caseName | directories | storage
        'empty response' | [:] | UNKNOWN
        'offline directory' | ['/private-path':new LogDirDescription(new KafkaStorageException('private-failure'),[:])] | UNAVAILABLE
        'capacity omitted by broker' | ['/private-path':new LogDirDescription(null,[:])] | VERIFIED
        'full disk' | ['/private-path':new LogDirDescription(null,[:],1000L,0L)] | VERIFIED
    }

    @Unroll
    void 'min ISR unknown #caseName never becomes a guessed default'() {
        given:
        fake.topicEntries = entries

        when:
        def t = service().readReplicationSnapshot(['orders'] as Set).topics().orders

        then:
        !t.minInSyncReplicas().present
        !t.toString().contains('private-')

        where:
        caseName | entries
        'missing' | []
        'null' | [entry('min.insync.replicas', null)]
        'sensitive' | [entry('min.insync.replicas','1',true)]
        'malformed' | [entry('min.insync.replicas','private-min-isr')]
        'zero' | [entry('min.insync.replicas','0')]
        'negative' | [entry('min.insync.replicas','-1')]
    }

    void 'authorization denied for configs becomes unknown for broker and min ISR'() {
        given:
        fake.configFailure = new ClusterAuthorizationException('private-config-response')

        when:
        def result = service().readReplicationSnapshot(['orders'] as Set)

        then:
        result.brokers().values().every { it.uncordoned() == UNKNOWN }
        !result.topics().orders.minInSyncReplicas().present
        !result.toString().contains('private-')
    }

    void 'captures active reassignment without changing or cancelling it'() {
        given:
        fake.active = [(new TopicPartition('orders',0)):new PartitionReassignment([0,1], [1], [])]

        expect:
        service().readReplicationSnapshot(['orders'] as Set).reassignments() == [new TopicPartition('orders',0)] as Set
    }

    @Unroll
    void 'fatal metadata error #stage is bounded safe and closes Admin'() {
        given:
        fake.failStage = stage

        when:
        service(100).readReplicationSnapshot(['orders'] as Set)

        then:
        def error = thrown(KafkaExecutionException)
        !(error.message + error.exceptionMessage).contains('private-')
        fake.calls.count('close') == 1

        where:
        stage << ['describeCluster','describeTopics','listPartitionReassignments','describeConfigs','describeLogDirs']
    }

    void 'unknown reassignment capability aborts instead of assuming no active work'() {
        given:
        fake.reassignmentFailure = new UnsupportedVersionException('private-unsupported')

        when:
        service().readReplicationSnapshot(['orders'] as Set)

        then:
        thrown(KafkaExecutionException)
        fake.calls.count('close') == 1
    }

    @Unroll
    void 'drift #kind during collection is rejected'() {
        given:
        fake.drift = kind

        when:
        service().readReplicationSnapshot(['orders'] as Set)

        then:
        thrown(KafkaExecutionException)
        fake.calls.count('close') == 1

        where:
        kind << ['cluster','topic']
    }

    void 'pending metadata obeys operation deadline'() {
        given:
        fake.pending = true

        when:
        service(20).readReplicationSnapshot(['orders'] as Set)

        then:
        thrown(KafkaExecutionException)
        fake.calls.count('close') == 1
    }

    void 'interrupt flag is preserved and Admin closed without exposing metadata'() {
        given:
        fake.pending = true
        Thread.currentThread().interrupt()

        when:
        service().readReplicationSnapshot(['orders'] as Set)

        then:
        thrown(KafkaExecutionException)
        Thread.currentThread().isInterrupted()
        fake.calls.count('close') == 1

        cleanup:
        Thread.interrupted()
    }

    void 'Admin initialization diagnostics cannot disclose credentials'() {
        given:
        def kafka = new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override protected Admin buildAdminClient() { throw new IllegalStateException('private-jaas-init') }
        }

        when:
        kafka.readReplicationSnapshot(['orders'] as Set)

        then:
        def error = thrown(KafkaExecutionException)
        !(error.message + error.exceptionMessage).contains('private-')
    }

    void 'ISR set order and partition response order do not cause artificial drift'() {
        given:
        fake.state = snapshot([orders:[[0,1],[1,2]]])
        fake.reorder = true

        expect:
        service().readReplicationSnapshot(['orders'] as Set).topics().orders.partitions()*.id() == [0,1]
    }

    void 'reassignment starting during collection prevents a stale preview'() {
        given:
        fake.drift = 'reassignment'

        when:
        service().readReplicationSnapshot(['orders'] as Set)

        then:
        thrown(KafkaExecutionException)
    }

    void 'real 3_9 snapshot can produce only a blocked preview with UNKNOWN fencing'() {
        given:
        def snapshot = service().readReplicationSnapshot(['orders'] as Set)

        when:
        def plans = new ReplicaAssignmentPlanner().plan(snapshot,[orders:2],
                com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.RACK_AWARE,false,false)
        def plan = new com.devshawn.kafka.gitops.domain.plan.DesiredPlan.Builder().addTopicPlans(
                new com.devshawn.kafka.gitops.domain.plan.TopicPlan.Builder().setName('orders')
                        .setAction(com.devshawn.kafka.gitops.enums.PlanAction.UPDATE).setReplicationPlan(plans.orders).build()).build()
        com.devshawn.kafka.gitops.util.PlanValidation.validateForApply(plan)

        then:
        thrown(com.devshawn.kafka.gitops.exception.ValidationException)
        snapshot.brokers().values().every { it.unfenced() == UNKNOWN }
    }

    void 'load includes hidden broker replicas and excludes future directory copies'() {
        given:
        fake.directoryFutures[3] = KafkaFuture.completedFuture(['/private-path':new LogDirDescription(null,
                [(new TopicPartition('hidden-by-topic-acl',0)):new ReplicaInfo(100L,0L,false),
                 (new TopicPartition('future-copy',0)):new ReplicaInfo(100L,0L,true)],1000L,500L)])

        when:
        def result = service().readReplicationSnapshot(['orders'] as Set)

        then:
        result.replicaCounts()[3] == 1L
        !result.toString().contains('hidden-by-topic-acl')
        !fake.calls.contains('listTopics')
    }

    static ConfigEntry entry(String key, String value, boolean sensitive = false) {
        new ConfigEntry(key,value,ConfigEntry.ConfigSource.DEFAULT_CONFIG,sensitive,false,[],ConfigEntry.ConfigType.STRING,null)
    }

    static KafkaFuture failed(Throwable failure) {
        def future = new KafkaFutureImpl()
        future.completeExceptionally(failure)
        future
    }
}

/** Any non-allowlisted call throws, including every Kafka write API. No network implementation exists. */
class ReadOnlyAdmin implements InvocationHandler {
    def state = snapshot([orders:[[0],[1]], '__internal':[[0,1,2]]])
    List<String> calls = []
    boolean internalRequested
    Map<Integer,List<ConfigEntry>> brokerEntries = (0..3).collectEntries { [(it):[ReplicationMetadataSpec.entry('cordoned.log.dirs','')]] }
    List<ConfigEntry> topicEntries = [ReplicationMetadataSpec.entry('min.insync.replicas','1'),
                                    ReplicationMetadataSpec.entry('sasl.jaas.config','private-full-config',true)]
    Map<Integer,KafkaFuture> directoryFutures = [:]
    Map active = [:]
    Throwable configFailure
    Throwable reassignmentFailure
    String failStage
    String drift
    boolean pending
    boolean reorder
    Admin admin = (Admin) Proxy.newProxyInstance(Admin.classLoader, [Admin] as Class[], this)

    @Override Object invoke(Object proxy, Method method, Object[] args) {
        String name = method.name
        if (name == 'toString') return 'ReadOnlyAdmin'
        calls.add(name)
        if (name == failStage) throw new IllegalStateException('private-response-text')
        if (name == 'close') {
            assert args[0] == Duration.ZERO
            return null
        }
        assert name in ['describeCluster','listTopics','describeTopics','listPartitionReassignments','describeConfigs','describeLogDirs'] : name
        def opts = args.last()
        assert opts.timeoutMs() > 0 && opts.timeoutMs() <= 2000
        switch(name) {
            case 'describeCluster':
                def nodes = state.brokers().values().collect { new Node(it.id(),'unused.invalid',1,it.rack().orElse(null)) }
                def clusterId = pending ? new KafkaFutureImpl() : KafkaFuture.completedFuture(
                        drift == 'cluster' && calls.count(name) > 1 ? 'different-id' : state.clusterId())
                return new DescribeClusterResult(KafkaFuture.completedFuture(nodes),KafkaFuture.completedFuture(nodes.first()),
                        clusterId,KafkaFuture.completedFuture([] as Set))
            case 'listTopics':
                internalRequested = opts.shouldListInternal()
                return new ListTopicsResult(KafkaFuture.completedFuture(state.topics().collectEntries { k,v -> [(k):new TopicListing(k, org.apache.kafka.common.Uuid.ZERO_UUID, false)] }))
            case 'describeTopics':
                def descriptions = args[0].collectEntries { n -> [(n):KafkaFuture.completedFuture(description(n,state.topics()[n],state.brokers()))] }
                if (reorder && calls.count(name) > 1) {
                    descriptions = args[0].collectEntries { n ->
                        def t = description(n,state.topics()[n],state.brokers())
                        [(n):KafkaFuture.completedFuture(new TopicDescription(n,false,t.partitions().reverse().collect { p ->
                            new org.apache.kafka.common.TopicPartitionInfo(p.partition(),p.leader(),p.replicas(),p.isr().reverse())
                        },[] as Set,t.topicId()))]
                    }
                }
                if (drift == 'topic' && calls.count(name) > 1) descriptions.clear()
                return new DescribeTopicsResult(null,descriptions)
            case 'listPartitionReassignments':
                if (drift == 'reassignment' && calls.count(name) > 1) active = [(new TopicPartition('orders',0)):new PartitionReassignment([0,1],[1],[])]
                return new ListPartitionReassignmentsResult(reassignmentFailure ? ReplicationMetadataSpec.failed(reassignmentFailure) : KafkaFuture.completedFuture(active))
            case 'describeConfigs':
                def futures = args[0].collectEntries { r ->
                    [(r):configFailure ? ReplicationMetadataSpec.failed(configFailure) : KafkaFuture.completedFuture(new Config(
                            r.type() == org.apache.kafka.common.config.ConfigResource.Type.TOPIC ? topicEntries : brokerEntries[r.name().toInteger()]))]
                }
                return new DescribeConfigsResult(futures)
            case 'describeLogDirs':
                def futures = state.brokers().keySet().collectEntries { int id ->
                    Map replicas = [:]
                    state.topics().each { topicName, topic -> topic.partitions().each { partition ->
                        if (partition.replicas().contains(id)) replicas[new TopicPartition(topicName,partition.id())] = new ReplicaInfo(100L,0L,false)
                    } }
                    [(id):directoryFutures.containsKey(id) ? directoryFutures[id] : KafkaFuture.completedFuture(
                            ['/private-log-path':new LogDirDescription(null,replicas,2000000L,1000000L)])]
                }
                return new DescribeLogDirsResult(futures)
        }
        throw new AssertionError(name)
    }
}
