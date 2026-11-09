package com.devshawn.kafka.gitops.manager;

import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.service.KafkaService;
import com.devshawn.kafka.gitops.util.LogUtil;
import com.devshawn.kafka.gitops.util.PlanValidation;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;

public class ApplyManager {

    private final ManagerConfig managerConfig;
    private final KafkaService kafkaService;

    public ApplyManager(ManagerConfig managerConfig, KafkaService kafkaService) {
        this.managerConfig = managerConfig;
        this.kafkaService = kafkaService;
    }

    public void applyTopics(DesiredPlan desiredPlan) {
        if (managerConfig.isSkipTopicsDisabled()) {
            return;
        }
        PlanValidation.validate(desiredPlan);
        // Check all partition preconditions before any topic is created, deleted or reconfigured.
        List<String> names = desiredPlan.getTopicPlans().stream().filter(t -> t.getPartitionPlan().isPresent())
                .map(TopicPlan::getName).toList();
        Map<String, TopicDescription> descriptions = kafkaService.describeTopics(names);
        desiredPlan.getTopicPlans().forEach(topic -> topic.getPartitionPlan().ifPresent(partitions -> {
            TopicDescription current = descriptions.get(topic.getName());
            if (current == null || current.partitions().isEmpty()) {
                throw new KafkaExecutionException("Partition preflight failed.", "Missing metadata for topic " + topic.getName() + ". Re-plan before retrying.");
            }
            int count = current.partitions().size();
            if (count != partitions.getPreviousCount() && count != partitions.getTargetCount()) {
                throw new KafkaExecutionException("Partition plan is stale.", "Topic " + topic.getName() + " has "
                        + count + " partitions; expected " + partitions.getPreviousCount() + " or already "
                        + partitions.getTargetCount() + ". Re-plan before retrying.");
            }
            topic.getTopicDetails().ifPresent(details -> LogUtil.printReplicationWarning(topic.getName(), details, current));
        }));
        desiredPlan.getTopicPlans().forEach(topicPlan -> {
            if (topicPlan.getAction() == PlanAction.ADD) {
                LogUtil.printTopicPreApply(topicPlan);
                kafkaService.createTopic(topicPlan.getName(), topicPlan.getTopicDetails().orElseThrow());
                LogUtil.printPostApply();
            } else if (topicPlan.getAction() == PlanAction.UPDATE) {
                LogUtil.printTopicPreApply(topicPlan);
                topicPlan.getPartitionPlan().ifPresent(partitions -> {
                    // Re-check immediately before the write; the service also verifies the resulting metadata.
                    boolean changed = kafkaService.increaseTopicPartitions(topicPlan.getName(), partitions.getPreviousCount(),
                            partitions.getTargetCount());
                    if (!changed) {
                        System.out.printf("Topic %s already has %s partitions; partition change is a no-op.%n",
                                topicPlan.getName(), partitions.getTargetCount());
                    }
                });
                topicPlan.getTopicConfigPlans().stream().filter(c -> c.getAction() != PlanAction.NO_CHANGE)
                        .forEach(topicConfigPlan -> applyTopicConfiguration(topicPlan, topicConfigPlan));
                LogUtil.printPostApply();
            } else if (topicPlan.getAction() == PlanAction.REMOVE && !managerConfig.isDeleteDisabled()) {
                LogUtil.printTopicPreApply(topicPlan);
                kafkaService.deleteTopic(topicPlan.getName());
                LogUtil.printPostApply();
            }
        });
    }

    private void applyTopicConfiguration(TopicPlan topicPlan, TopicConfigPlan topicConfigPlan) {
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicPlan.getName());
        List<AlterConfigOp> configOps = new ArrayList<>();

        ConfigEntry configEntry = new ConfigEntry(topicConfigPlan.getKey(), topicConfigPlan.getValue().orElse(null));

        // TODO: Make OpType work with append/subtract
        if (topicConfigPlan.getAction() == PlanAction.ADD) {
            configOps.add(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET));
        } else if (topicConfigPlan.getAction() == PlanAction.UPDATE) {
            configOps.add(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET));
        } else if (topicConfigPlan.getAction() == PlanAction.REMOVE) {
            configOps.add(new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE));
        }

        configs.put(configResource, configOps);

        kafkaService.updateTopicConfig(configs);
    }

    public void applyAcls(DesiredPlan desiredPlan) {
        desiredPlan.getAclPlans().forEach(aclPlan -> {
            if (aclPlan.getAction() == PlanAction.ADD) {
                LogUtil.printAclPreApply(aclPlan);
                kafkaService.createAcl(aclPlan.getAclDetails().toAclBinding());
                LogUtil.printPostApply();
            } else if (aclPlan.getAction() == PlanAction.REMOVE && !managerConfig.isDeleteDisabled()) {
                LogUtil.printAclPreApply(aclPlan);
                kafkaService.deleteAcl(aclPlan.getAclDetails().toAclBinding());
                LogUtil.printPostApply();
            }
        });
    }
}
