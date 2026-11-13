// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import ch.qos.logback.classic.Logger
import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.exception.ValidationException
import com.devshawn.kafka.gitops.service.ParserService
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import static com.devshawn.kafka.gitops.PlanTestSupport.*

class PartitionWorkflowSpec extends Specification {
    @TempDir File directory
    def root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    def kafkaLogger = (Logger) LoggerFactory.getLogger('org.apache.kafka')
    def oldRoot = root.level
    def oldKafka = kafkaLogger.level

    void cleanup() {
        root.level = oldRoot
        kafkaLogger.level = oldKafka
    }

    @Unroll
    void 'complete workflow preserves skip-topics=#skipTopics skip-acls=#skipAcls verbose=#verbose'() {
        given:
        File file = state(6)
        def config = new ManagerConfig.Builder().mergeFrom(options()).setStateFile(file)
                .setSkipTopicsDisabled(skipTopics).setSkipAclsDisabled(skipAcls).setVerboseRequested(verbose).build()
        def kafka = new TestKafkaService(partitionCounts: ['test-topic': 3])
        def manager = new StateManager(config, new ParserService(file), kafka)

        when:
        def planned = manager.plan()
        manager.apply()

        then:
        planned.topicPlans.empty == skipTopics
        planned.aclPlans.empty == skipAcls
        kafka.partitionChanges.empty == skipTopics
        kafka.acls.empty == skipAcls
        skipTopics ? kafka.metadataReads.empty : kafka.partitionCounts['test-topic'] == 6

        where:
        skipTopics | skipAcls | verbose
        false      | true     | false
        false      | true     | true
        true       | false    | false
        true       | false    | true
        false      | false    | false
        false      | false    | true
    }

    @Unroll
    void 'state validation rejects partitions #partitions before metadata queries'() {
        given:
        File file = state(partitions)
        def config = new ManagerConfig.Builder().mergeFrom(options()).setStateFile(file).build()
        def kafka = new TestKafkaService()

        when:
        new StateManager(config, new ParserService(file), kafka).plan()

        then:
        def error = thrown(ValidationException)
        error.message.contains('positive integer')
        kafka.metadataReads.empty
        kafka.partitionChanges.empty

        where:
        partitions << [0, -1]
    }

    private File state(int partitions) {
        File file = new File(directory, 'state.yaml')
        file.text = """topics:
  test-topic:
    partitions: ${partitions}
    replication: 1
services:
  application:
    type: application
    principal: User:test
    consumes:
      - test-topic
"""
        file
    }
}
