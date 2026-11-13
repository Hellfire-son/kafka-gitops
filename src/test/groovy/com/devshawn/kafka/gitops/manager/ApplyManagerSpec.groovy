// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.TestKafkaService
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.util.PlanUtil
import org.apache.kafka.clients.admin.AlterConfigOp
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class ApplyManagerSpec extends Specification {
    @Unroll
    void 'previousValue does not change #action apply semantics'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        def desired = plan([config('cleanup.policy', action, value, 'old-diagnostic-value')])

        when:
        new ApplyManager(options(), kafka).applyTopics(desired)

        then:
        kafka.operations.size() == 1
        kafka.operations.first().operation.opType() == operation
        kafka.operations.first().operation.configEntry().value() == value

        where:
        action | value    | operation
        ADD    | 'delete' | AlterConfigOp.OpType.SET
        UPDATE | 'delete' | AlterConfigOp.OpType.SET
        REMOVE | null     | AlterConfigOp.OpType.DELETE
    }

    void 'sensitive values remain available to in-memory apply'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        def desired = plan([config('custom.option', UPDATE, 'private-apply-value', null, true)])

        when:
        new ApplyManager(options(), kafka).applyTopics(desired)

        then:
        kafka.operations.first().operation.configEntry().value() == 'private-apply-value'
    }

    void 'no-delete keeps topics while config override removal still uses DELETE'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        def desired = new DesiredPlan.Builder().mergeFrom(plan([config('cleanup.policy', REMOVE, null, 'delete')]))
                .addTopicPlans(new TopicPlan.Builder().setName('keep-topic').setAction(REMOVE).build()).build()

        when:
        new ApplyManager(options(null, true), kafka).applyTopics(desired)

        then:
        kafka.deletedTopics.empty
        kafka.operations.first().operation.opType() == AlterConfigOp.OpType.DELETE
    }

    void 'topic creation keeps original confidential config values'() {
        given:
        TestKafkaService kafka = new TestKafkaService()
        def original = details(['custom.password': 'private-apply-new-topic'])

        when:
        new ApplyManager(options(), kafka).applyTopics(plan([], original, ADD))

        then:
        kafka.createdTopics == [original]
        kafka.createdTopics.first().configs['custom.password'] == 'private-apply-new-topic'
    }

    void 'overview continues to honor skip-topics and skip-acls'() {
        given:
        def desired = plan([config('retention.ms', UPDATE, '2', '1')])

        expect:
        PlanUtil.getOverview(desired, false, false, true).update == 0
        PlanUtil.getOverview(desired, false, true, false).update == 1
    }
}
