// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.config.ManagerConfig
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan
import com.devshawn.kafka.gitops.domain.plan.TopicPlan
import com.devshawn.kafka.gitops.domain.state.TopicDetails
import com.devshawn.kafka.gitops.enums.PlanAction
import com.devshawn.kafka.gitops.service.KafkaService
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.config.ConfigResource

class PlanTestSupport {
    static ObjectMapper mapper() {
        JsonMapper.builderWithJackson2Defaults().build()
    }

    static ManagerConfig options(File planFile = null, boolean deleteDisabled = false, boolean includeUnchanged = false) {
        new ManagerConfig.Builder().setVerboseRequested(false).setDeleteDisabled(deleteDisabled)
                .setIncludeUnchangedEnabled(includeUnchanged).setSkipAclsDisabled(false).setSkipTopicsDisabled(false)
                .setStateFile(new File('unused-state.yaml')).setNullablePlanFile(planFile).build()
    }

    static TopicDetails details(Map<String, String> configs = [:]) {
        new TopicDetails.Builder().setPartitions(1).setReplication(1).putAllConfigs(configs).build()
    }

    static ConfigEntry entry(String key, String value, boolean sensitive = false,
                             ConfigEntry.ConfigSource source = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
        new ConfigEntry(key, value, source, sensitive, false, [], ConfigEntry.ConfigType.STRING, null)
    }

    static TopicConfigPlan config(String key, PlanAction action, String value = null, String previous = null,
                                  boolean sensitive = false) {
        new TopicConfigPlan.Builder().setKey(key).setAction(action).setNullableValue(value)
                .setNullablePreviousValue(previous).setSensitive(sensitive).build()
    }

    static DesiredPlan plan(List<TopicConfigPlan> configs, TopicDetails details = null, PlanAction action = PlanAction.UPDATE) {
        new DesiredPlan.Builder().addTopicPlans(new TopicPlan.Builder().setName('test-topic').setAction(action)
                .setNullableTopicDetails(details).addAllTopicConfigPlans(configs).build()).build()
    }
}

// Every Kafka operation is handled in memory. No AdminClient is ever constructed.
class TestKafkaService extends KafkaService {
    List<TopicListing> topics = [new TopicListing('test-topic', org.apache.kafka.common.Uuid.ZERO_UUID, false)]
    List<ConfigEntry> entries = []
    Map<String, List<ConfigEntry>> topicEntries = [:]
    List<Map> operations = []
    List<String> deletedTopics = []
    List<TopicDetails> createdTopics = []
    List<AclBinding> acls = []
    Map<String, Integer> partitionCounts = [:]
    Map<String, Integer> replicationCounts = [:]
    List<Map> partitionChanges = []
    List<List<String>> metadataReads = []

    TestKafkaService() {
        super(new KafkaGitopsConfig.Builder().build())
    }

    @Override
    List<TopicListing> getTopics() { topics }

    @Override
    Map<String, TopicDescription> describeTopics(List<String> names) {
        metadataReads.add(new ArrayList<>(names))
        names.findAll { name -> topics.any { it.name() == name } }.collectEntries { name ->
            def nodes = (0..<replicationCounts.getOrDefault(name, 1)).collect { new Node(it, 'unused-host', 1) }
            [(name): new TopicDescription(name, false, (0..<partitionCounts.getOrDefault(name, 1)).collect {
                new TopicPartitionInfo(it, nodes.first(), nodes, nodes)
            })]
        }
    }

    @Override
    boolean increaseTopicPartitions(String name, int previousCount, int targetCount) {
        if (partitionCounts.getOrDefault(name, 1) == targetCount) {
            return false
        }
        partitionChanges.add([name: name, previousCount: previousCount, targetCount: targetCount])
        partitionCounts[name] = targetCount
        true
    }

    @Override
    Map<ConfigResource, Config> describeConfigsForTopics(List<String> names) {
        entries == null ? [:] : names.collectEntries {
            [(new ConfigResource(ConfigResource.Type.TOPIC, it)): new Config(topicEntries.getOrDefault(it, entries))]
        }
    }

    @Override
    void updateTopicConfig(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        configs.each { resource, changes -> changes.each { operations.add([resource: resource, operation: it]) } }
    }

    @Override
    void createTopic(String name, TopicDetails details) { createdTopics.add(details) }

    @Override
    void deleteTopic(String name) { deletedTopics.add(name) }

    @Override
    List<AclBinding> getAcls() { acls }

    @Override
    void createAcl(AclBinding acl) { acls.add(acl) }

    @Override
    void deleteAcl(AclBinding acl) { acls.remove(acl) }
}
