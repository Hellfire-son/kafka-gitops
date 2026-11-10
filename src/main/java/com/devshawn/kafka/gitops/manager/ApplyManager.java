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
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException;
import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.service.ReplicationSafetyPolicy;
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
        validatePermissions();
        if (managerConfig.isSkipTopicsDisabled()) {
            return;
        }
        PlanValidation.validate(desiredPlan);
        preflightReplication(desiredPlan);
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
        validatePermissions();
        if (desiredPlan.getTopicPlans().stream().anyMatch(t -> t.getReplicationPlan().isPresent())
                && desiredPlan.getAclPlans().stream().anyMatch(a -> a.getAction() == PlanAction.ADD
                || (a.getAction() == PlanAction.REMOVE && !managerConfig.isDeleteDisabled()))) {
            PlanValidation.validate(desiredPlan);
            preflightReplication(desiredPlan);
        }
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

    private void validatePermissions() {
        if (managerConfig.isAllowReplicationDecreaseRequested() && !managerConfig.isManageReplicationRequested()) {
            throw new ValidationException("--allow-replication-decrease requires --manage-replication.");
        }
    }

    /** Batch preflight precedes every topic/ACL write. It always stops at the execution boundary in this stage. */
    private void preflightReplication(DesiredPlan plan) {
        List<TopicPlan> topics = plan.getTopicPlans().stream().filter(t -> t.getReplicationPlan().isPresent())
                .sorted(Comparator.comparing(TopicPlan::getName)).toList();
        if (topics.isEmpty()) return;
        ReplicationSafetyPolicy policy = new ReplicationSafetyPolicy();
        var permissions = new ReplicationSafetyPolicy.Permissions(managerConfig.isManageReplicationRequested(),
                managerConfig.isAllowReplicationDecreaseRequested(), managerConfig.isDeleteDisabled());
        Map<String, ReplicationSafetyResult> results = new TreeMap<>();
        Map<String, List<ReplicationSafetyResult.Finding>> permissionFindings = new TreeMap<>();
        for (TopicPlan topic : topics) {
            var reasons = policy.permissions(topic.getReplicationPlan().orElseThrow(), permissions);
            permissionFindings.put(topic.getName(), reasons);
        }
        if (permissionFindings.values().stream().anyMatch(reasons -> !reasons.isEmpty())) {
            topics.forEach(topic -> results.put(topic.getName(), policy.blockedBeforeMetadata(
                    topic.getReplicationPlan().orElseThrow(), permissionFindings.get(topic.getName()))));
            throw new ReplicationPreflightException(results);
        }
        long started = System.nanoTime();
        ReplicationClusterSnapshot snapshot;
        try {
            snapshot = kafkaService.readReplicationPreflightSnapshot(new TreeSet<>(permissionFindings.keySet()));
        } catch (RuntimeException ex) {
            topics.forEach(topic -> results.put(topic.getName(), policy.blockedBeforeMetadata(topic.getReplicationPlan().orElseThrow(),
                    List.of(ReplicationSafetyResult.Finding.global(ReplicationSafetyResult.Reason.METADATA_UNAVAILABLE,
                            ReplicationSafetyResult.Condition.UNKNOWN)))));
            throw new ReplicationPreflightException(results);
        }
        long now = System.nanoTime();
        for (TopicPlan topic : topics) {
            results.put(topic.getName(), policy.evaluate(topic.getName(), topic.getReplicationPlan().orElseThrow(), snapshot,
                    permissions, ReplicationSafetyPolicy.Constraints.forTopic(topic, started, now)));
        }
        throw new ReplicationPreflightException(results);
    }
}
