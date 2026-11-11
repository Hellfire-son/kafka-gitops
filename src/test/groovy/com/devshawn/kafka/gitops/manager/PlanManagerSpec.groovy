package com.devshawn.kafka.gitops.manager

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.state.DesiredState
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.TopicListing
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

@Unroll
class PlanManagerSpec extends Specification {
    TestKafkaService kafka = new TestKafkaService()
    Logger logger
    Level previousLevel
    ListAppender<ILoggingEvent> appender

    void setup() {
        logger = (Logger) LoggerFactory.getLogger(PlanManager)
        previousLevel = logger.level
        logger.setLevel(Level.INFO)
        appender = new ListAppender<>()
        appender.start()
        logger.addAppender(appender)
    }

    void cleanup() {
        logger.detachAppender(appender)
        logger.setLevel(previousLevel)
        appender.stop()
    }

    void 'plans #description with correct previous value'() {
        given:
        kafka.entries = entries
        def state = new DesiredState.Builder().putTopics('test-topic', details(desired)).build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(state, builder)
        def topic = builder.build().topicPlans.first()
        def change = topic.topicConfigPlans.first()

        then:
        topic.action == (action == NO_CHANGE ? NO_CHANGE : UPDATE)
        change.action == action
        change.previousValue.orElse(null) == previous
        change.value.orElse(null) == value
        change.sensitive == sensitive
        !appender.list.collect { it.formattedMessage }.join('\n').contains('private-test')

        where:
        description          | entries                                                 | desired                               | action    | previous   | value                | sensitive
        'ADD'                | []                                                      | ['message.max.bytes': '2097152']       | ADD       | null       | '2097152'            | false
        'UPDATE'             | [entry('retention.ms', '86400000')]                       | ['retention.ms': '172800000']          | UPDATE    | '86400000' | '172800000'          | false
        'REMOVE delete text' | [entry('cleanup.policy', 'delete')]                       | [:]                                   | REMOVE    | 'delete'   | null                 | false
        'NO_CHANGE'          | [entry('cleanup.policy', 'delete')]                       | ['cleanup.policy': 'delete']          | NO_CHANGE | null       | 'delete'             | false
        'unknown UPDATE'     | [entry('retention.ms', null)]                             | ['retention.ms': '172800000']          | UPDATE    | null       | '172800000'          | false
        'unknown REMOVE'     | [entry('retention.ms', null)]                             | [:]                                   | REMOVE    | null       | null                 | false
        'sensitive UPDATE'   | [entry('custom.option', null, true)]                      | ['custom.option': 'private-test-new']  | UPDATE    | null       | 'private-test-new'   | true
        'sensitive REMOVE'   | [entry('custom.option', 'private-test-old', true)]        | [:]                                   | REMOVE    | null       | null                 | true
        'secret name'        | [entry('custom.password', 'private-test-old')]            | [:]                                   | REMOVE    | null       | null                 | true
        'unknown extension'  | [entry('custom.option', 'private-test-old')]              | ['custom.option': 'private-test-new']  | UPDATE    | null       | 'private-test-new'   | true
        'sensitive core key' | [entry('retention.ms', 'private-test-old', true)]          | [:]                                   | REMOVE    | null       | null                 | true
        'inherited sensitive'| [entry('custom.option', null, true, ConfigEntry.ConfigSource.DEFAULT_CONFIG)] | ['custom.option': 'private-test-new'] | ADD | null | 'private-test-new' | true
    }

    void 'handles several changes and keeps previous values when filtering unchanged configs'() {
        given:
        kafka.entries = [entry('retention.ms', '86400000'), entry('cleanup.policy', 'delete'), entry('segment.bytes', '100000')]
        def state = new DesiredState.Builder().putTopics('test-topic', details([
                'retention.ms': '172800000', 'message.max.bytes': '2097152', 'segment.bytes': '100000'])).build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(state, builder)
        def plan = builder.build()
        def changes = plan.toChangesOnlyPlan().topicPlans.first().topicConfigPlans.collectEntries { [(it.key): it] }

        then:
        plan.topicPlans.first().topicConfigPlans.size() == 4
        changes.keySet() == ['retention.ms', 'cleanup.policy', 'message.max.bytes'] as Set
        changes['retention.ms'].previousValue.get() == '86400000'
        changes['cleanup.policy'].previousValue.get() == 'delete'
        changes['message.max.bytes'].action == ADD
    }

    void 'missing topic config metadata does not cause a null pointer exception'() {
        given:
        kafka.entries = null
        def state = new DesiredState.Builder().putTopics('test-topic', details(['retention.ms': '1000'])).build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(options(), kafka, mapper()).planTopics(state, builder)

        then:
        builder.build().topicPlans.first().topicConfigPlans.first().action == ADD
    }

    void 'preserves include and exclude prefixes and no-delete=#noDelete'() {
        given:
        kafka.topics = ['test-topic', 'managed-delete', 'managed-keep', 'external-topic'].collect { new TopicListing(it, org.apache.kafka.common.Uuid.ZERO_UUID, false) }
        def state = new DesiredState.Builder().putTopics('test-topic', details())
                .addPrefixedTopicsToAccept('managed-').addPrefixedTopicsToIgnore('managed-keep').build()
        def builder = new DesiredPlan.Builder()

        when:
        new PlanManager(options(null, noDelete), kafka, mapper()).planTopics(state, builder)

        then:
        builder.build().topicPlans.findAll { it.action == REMOVE }*.name == (noDelete ? [] : ['managed-delete'])

        where:
        noDelete << [false, true]
    }
}
