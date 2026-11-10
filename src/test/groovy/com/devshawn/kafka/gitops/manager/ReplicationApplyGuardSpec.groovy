package com.devshawn.kafka.gitops.manager

import com.devshawn.kafka.gitops.domain.plan.*
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.exception.ValidationException
import spock.lang.Specification
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*
import static com.devshawn.kafka.gitops.ReplicationTestSupport.*
import static com.devshawn.kafka.gitops.enums.PlanAction.*

class ReplicationApplyGuardSpec extends Specification {
    @Unroll
    void 'RF plan fails before any Kafka access decrease=#decrease noDelete=#noDelete'() {
        given:
        def rf = decrease ? replication([[0, 1, 2]], [[0, 1]], 2) : replication()
        def create = new TopicPlan.Builder().setName('new-topic').setAction(ADD)
                .setTopicDetails(new TopicDetails.Builder().setPartitions(1).setReplication(1).build()).build()
        def configs = new TopicPlan.Builder().setName('config-topic').setAction(UPDATE)
                .addTopicConfigPlans(config('retention.ms', UPDATE, '2000', '1000')).build()
        def plan = new DesiredPlan.Builder().addTopicPlans(create, configs, topic(rf)).build()

        when:
        // A null service makes any accidental metadata or mutation call fail this test.
        new ApplyManager(options(null, noDelete), null).applyTopics(plan)

        then:
        def error = thrown(ValidationException)
        error.message.contains('MANAGE_REPLICATION_REQUIRED')
        error.message.contains('No topic operations were started')

        where:
        [decrease, noDelete] << [[false, true], [false, true]].combinations()
    }

    void 'skip-topics retains existing behavior even for an RF plan'() {
        given:
        def options = new com.devshawn.kafka.gitops.config.ManagerConfig.Builder().mergeFrom(options())
                .setSkipTopicsDisabled(true).build()

        when:
        new ApplyManager(options, null).applyTopics(desired())

        then:
        noExceptionThrown()
    }
}
