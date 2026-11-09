package com.devshawn.kafka.gitops.util

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

@Unroll
class LogUtilSpec extends Specification {
    void 'shows #action as an explicit transition'() {
        expect:
        output(plan([config(key, action, value, previous)])).contains(expected)

        where:
        action | key                 | value       | previous   | expected
        ADD    | 'message.max.bytes' | '2097152'   | null       | '+ message.max.bytes: [default] -> 2097152'
        UPDATE | 'retention.ms'      | '172800000' | '86400000' | '~ retention.ms: 86400000 -> 172800000'
        REMOVE | 'cleanup.policy'    | null        | 'delete'   | '- cleanup.policy: delete -> [default]'
        UPDATE | 'cleanup.policy'    | 'delete'    | 'compact'  | '~ cleanup.policy: compact -> delete'
        UPDATE | 'retention.ms'      | '1000'      | null       | '~ retention.ms: [unknown] -> 1000'
        REMOVE | 'cleanup.policy'    | null        | null       | '- cleanup.policy: [unknown] -> [default]'
        ADD    | 'retention.ms'      | null        | null       | '+ retention.ms: [default] -> [unknown]'
    }

    void 'shows simultaneous changes and omits unchanged configs'() {
        when:
        String text = output(plan([config('message.max.bytes', ADD, '2097152'),
                                  config('retention.ms', UPDATE, '172800000', '86400000'),
                                  config('cleanup.policy', REMOVE, null, 'delete'),
                                  config('segment.bytes', NO_CHANGE, '100000')]))

        then:
        text.contains('+ message.max.bytes: [default] -> 2097152')
        text.contains('~ retention.ms: 86400000 -> 172800000')
        text.contains('- cleanup.policy: delete -> [default]')
        !text.contains('segment.bytes')
    }

    void 'redacts both sides of sensitive #action changes'() {
        when:
        String text = output(plan([config('custom.option', action, 'private-log-new', 'private-log-old', true)]))

        then:
        !text.contains('private-log')
        text.contains(expected)

        where:
        action | expected
        ADD    | '[default] -> [redacted]'
        UPDATE | '[redacted] -> [redacted]'
        REMOVE | '[redacted] -> [default]'
    }

    void 'protects legacy plans by key name and new topic values in both plan and pre-apply output'() {
        given:
        def plans = [plan([config('sasl.jaas.config', UPDATE, 'private-log-jaas', 'private-log-old')]),
                     plan([], details(['ssl.key.password': 'private-log-ssl', 'cleanup.policy': 'delete']), ADD)]

        expect:
        plans.every { desired ->
            String text = output(desired)
            !text.contains('private-log') && text.contains('[redacted]')
        }
        plans.every { desired -> !output(desired, true).contains('private-log') }
    }

    void 'unknown extension values are confidential without broker metadata'() {
        expect:
        !output(plan([config('custom.option', UPDATE, 'private-unknown-new', 'private-unknown-old')])).contains('private-unknown')
        !output(plan([], details(['custom.option': 'private-unknown-new']), ADD)).contains('private-unknown')
    }

    private String output(DesiredPlan desired, boolean preApply = false) {
        PrintStream original = System.out
        ByteArrayOutputStream bytes = new ByteArrayOutputStream()
        try {
            System.setOut(new PrintStream(bytes, true, 'UTF-8'))
            if (preApply) {
                LogUtil.printTopicPreApply(desired.topicPlans.first())
            } else {
                LogUtil.printPlan(desired, false, false, false)
            }
            bytes.toString('UTF-8')
        } finally {
            System.setOut(original)
        }
    }
}
