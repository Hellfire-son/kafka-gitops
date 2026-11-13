// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import ch.qos.logback.classic.Logger
import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.manager.PlanningKafka
import com.devshawn.kafka.gitops.service.ParserService
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*

class ReplicationWorkflowSpec extends Specification {
    @TempDir File directory
    def root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    def kafkaLogger = (Logger) LoggerFactory.getLogger('org.apache.kafka')
    def oldRoot = root.level
    def oldKafka = kafkaLogger.level

    void cleanup() { root.level = oldRoot; kafkaLogger.level = oldKafka }

    @Unroll
    void 'end-to-end planning and saved JSON preserve apply boundary verbose=#verbose noDelete=#noDelete'() {
        given:
        File state = new File(directory,'state.yaml')
        state.text = 'topics:\n  orders:\n    partitions: 2\n    replication: 2\n'
        File plan = new File(directory,'plan.json')
        def config = new ManagerConfig.Builder().mergeFrom(options(plan,noDelete)).setStateFile(state)
                .setManageReplicationRequested(true).setVerboseRequested(verbose).setSkipAclsDisabled(true).build()
        def kafka = new PlanningKafka()
        def manager = new StateManager(config,new ParserService(state),kafka)

        when:
        def planned = manager.plan()

        then:
        plan.exists()
        planned.topicPlans.first().replicationPlan.present
        !plan.text.contains('allowReplicationDecrease')
        kafka.snapshotReads.size() == 1

        when:
        manager.apply()

        then:
        thrown(ValidationException)
        kafka.createdTopics.isEmpty()
        kafka.operations.isEmpty()
        kafka.deletedTopics.isEmpty()
        kafka.partitionChanges.isEmpty()

        where:
        [verbose,noDelete] << [[false,true],[false,true]].combinations()
    }
}
