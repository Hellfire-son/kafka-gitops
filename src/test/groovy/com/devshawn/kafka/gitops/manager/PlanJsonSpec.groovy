package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan
import com.devshawn.kafka.gitops.exception.ReadPlanInputException
import com.devshawn.kafka.gitops.exception.WritePlanOutputException
import tools.jackson.databind.cfg.EnumFeature
import tools.jackson.databind.DeserializationFeature
import tools.jackson.databind.DatabindException
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

@Unroll
class PlanJsonSpec extends Specification {
    @TempDir
    File temporaryDirectory

    void 'reads old #action JSON without previousValue and preserves its apply value'() {
        given:
        File file = jsonFile("""{"topicPlans":[{"name":"test-topic","action":"UPDATE","topicConfigPlans":[
          {"key":"retention.ms","action":"${action}","value":${mapper().writeValueAsString(value)}}]}],"aclPlans":[]}""")

        when:
        def restored = new PlanManager(options(file), null, mapper()).readPlanFromFile()

        then:
        restored.topicPlans.first().topicConfigPlans.first().previousValue.empty
        restored.topicPlans.first().topicConfigPlans.first().value.orElse(null) == value
        !restored.topicPlans.first().topicConfigPlans.first().sensitive

        where:
        action | value
        ADD    | '172800000'
        UPDATE | '172800000'
        REMOVE | null
    }

    void 'reads existing repository plan fixture #fixture'() {
        given:
        File file = new File(getClass().getResource('/plans/' + fixture).toURI())

        expect:
        new PlanManager(options(file), null, mapper()).readPlanFromFile() != null

        where:
        fixture << new File('src/test/resources/plans').listFiles()
                .findAll { it.name.endsWith('-plan.json') && it.name != 'invalid-plan.json' }*.name.sort()
    }

    void 'round trips ADD UPDATE REMOVE and unchanged with previous values'() {
        given:
        def original = plan([
                config('message.max.bytes', ADD, '2097152'),
                config('retention.ms', UPDATE, '172800000', '86400000'),
                config('cleanup.policy', REMOVE, null, 'delete'),
                config('segment.bytes', NO_CHANGE, '100000')], details(['retention.ms': '172800000']))
        File file = new File(temporaryDirectory, 'plan.json')
        def manager = new PlanManager(options(file, false, true), null, mapper())

        when:
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()
        def json = mapper().readTree(file)

        then:
        restored == original
        json.topicPlans[0].topicConfigPlans[1].previousValue.asText() == '86400000'
        json.topicPlans[0].topicConfigPlans[2].previousValue.asText() == 'delete'
        !json.topicPlans[0].topicConfigPlans[0].has('previousValue')
    }

    void 'changes-only JSON keeps previous values and omits unchanged configs'() {
        given:
        def original = plan([config('retention.ms', UPDATE, '2', '1'), config('segment.bytes', NO_CHANGE, '100000')])
        File file = new File(temporaryDirectory, 'changes.json')
        def manager = new PlanManager(options(file), null, mapper())

        when:
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()

        then:
        restored.topicPlans.first().topicConfigPlans.size() == 1
        restored.topicPlans.first().topicConfigPlans.first().previousValue.get() == '1'
    }

    void 'exports sensitive REMOVE safely and preserves its metadata'() {
        given:
        def original = plan([config('custom.option', REMOVE, null, null, true)], details())
        File file = new File(temporaryDirectory, 'remove.json')
        def manager = new PlanManager(options(file), null, mapper())

        when:
        manager.writePlanToFile(original)
        def restored = manager.readPlanFromFile()

        then:
        restored == original
        restored.topicPlans.first().topicConfigPlans.first().sensitive
        !file.text.contains('previousValue')
    }

    void 'refuses to export #description secrets before touching the destination'() {
        given:
        File file = new File(temporaryDirectory, 'protected.json')
        file.text = 'existing-file-content'
        def manager = new PlanManager(options(file, false, true), null, mapper())

        when:
        manager.writePlanToFile(original)

        then:
        def error = thrown(WritePlanOutputException)
        !error.message.contains('private-json')
        file.text == 'existing-file-content'

        where:
        description        | original
        'metadata marked'  | plan([config('custom.option', UPDATE, 'private-json-new', null, true)])
        'previous value'   | plan([config('custom.option', REMOVE, null, 'private-json-old', true)])
        'duplicate details'| plan([config('custom.option', UPDATE, null, null, true)], details(['custom.option': 'private-json-new']))
        'new topic'        | plan([], details(['custom.password': 'private-json-new']), ADD)
        'legacy key name'  | plan([config('sasl.jaas.config', UPDATE, 'private-json-jaas')])
    }

    void 'direct model serialization also refuses sensitive values'() {
        when:
        mapper().writeValueAsString(config('custom.option', UPDATE, 'private-json-new', null, true))

        then:
        def error = thrown(DatabindException)
        !error.message.contains('private-json-new')
    }

    void 'refused confidential export does not create a new file'() {
        given:
        File file = new File(temporaryDirectory, 'absent.json')

        when:
        new PlanManager(options(file), null, mapper()).writePlanToFile(plan([config('custom.password', UPDATE, 'private-json-new')]))

        then:
        thrown(WritePlanOutputException)
        !file.exists()
    }

    void 'strictly rejects incompatible JSON #description even with a permissive shared mapper'() {
        given:
        File file = jsonFile(json)
        def permissive = mapper().rebuild().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(EnumFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
                .build()

        when:
        new PlanManager(options(file), null, permissive).readPlanFromFile()

        then:
        def error = thrown(ReadPlanInputException)
        !error.message.contains('private-invalid')
        !permissive.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

        where:
        description                | json
        'unknown operation'        | '{"topicPlans":[{"name":"x","action":"private-invalid"}]}'
        'numeric operation'        | '{"topicPlans":[{"name":"x","action":0}]}'
        'missing operation'        | '{"topicPlans":[{"name":"x"}]}'
        'unknown config operation' | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[{"key":"x","action":"APPEND","value":"1"}]}]}'
        'upstream topicDetailsPlan'| '{"topicPlans":[{"name":"x","action":"UPDATE","topicDetailsPlan":{"partitions":2}}]}'
        'unknown config field'     | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[{"key":"x","action":"UPDATE","value":"1","newAction":"private-invalid"}]}]}'
        'missing SET value'        | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[{"key":"x","action":"UPDATE"}]}]}'
        'null SET value'           | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[{"key":"x","action":"ADD","value":null}]}]}'
        'null sensitivity'         | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[{"key":"x","action":"UPDATE","value":"1","sensitive":null}]}]}'
        'empty UPDATE'             | '{"topicPlans":[{"name":"x","action":"UPDATE","topicConfigPlans":[]}]}'
        'missing create details'   | '{"topicPlans":[{"name":"x","action":"ADD"}]}'
        'ignored config changes'   | '{"topicPlans":[{"name":"x","action":"NO_CHANGE","topicConfigPlans":[{"key":"x","action":"ADD","value":"1"}]}]}'
        'unsupported ACL UPDATE'   | '{"aclPlans":[{"name":"x","action":"UPDATE","aclDetails":{"name":"x","type":"TOPIC","pattern":"LITERAL","principal":"User:x","host":"*","operation":"READ","permission":"ALLOW"}}]}'
        'wrong collection type'    | '{"topicPlans":{}}'
        'unknown root field'       | '{"topicPlans":[],"private-invalid":[]}'
        'duplicate field'          | '{"topicPlans":[],"topicPlans":[]}'
        'null root'                | 'null'
        'trailing document'        | '{"topicPlans":[],"aclPlans":[]} {"private-invalid":true}'
    }

    private File jsonFile(String json) {
        File file = new File(temporaryDirectory, 'input.json')
        file.text = json
        file
    }
}
