// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.manager;

import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport.Step;
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
import com.devshawn.kafka.gitops.service.ReplicationApplication;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor;
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.function.BooleanSupplier;

import static com.devshawn.kafka.gitops.domain.plan.ApplyReport.Result.*;

public class ApplyManager {

    private final ManagerConfig managerConfig;
    private final KafkaService kafkaService;
    private final ReplicationApplication replicationApplication;

    public ApplyManager(ManagerConfig managerConfig, KafkaService kafkaService) {
        this(managerConfig, kafkaService, null);
    }

    /** Explicit integration seam, also bound by the CLI to the fresh preflight/executor application. */
    public ApplyManager(ManagerConfig managerConfig, KafkaService kafkaService, ReplicationApplication replicationApplication) {
        this.managerConfig = managerConfig;
        this.kafkaService = kafkaService;
        this.replicationApplication = replicationApplication;
    }

    public void applyPlan(DesiredPlan plan) {
        applyPlan(plan, null);
    }

    public void applyPlan(DesiredPlan plan, ApplyReport report) {
        boolean rf = plan.getTopicPlans().stream().anyMatch(t -> t.getReplicationPlan().isPresent());
        validateReplicationPermissions(plan);
        if (replicationApplication == null || !rf || managerConfig.isSkipTopicsDisabled()) {
            if (!managerConfig.isSkipTopicsDisabled()) applyTopics(plan, report);
            if (!managerConfig.isSkipAclsDisabled()) applyAcls(plan, report);
            return;
        }
        applyWithReplication(plan, report);
    }

    /** Permission-only gate: safe before no-change filtering, metadata access or any write. */
    public void validateReplicationPermissions(DesiredPlan plan) {
        boolean rf = plan.getTopicPlans().stream().anyMatch(t -> t.getReplicationPlan().isPresent());
        validatePermissions();
        if (rf && !managerConfig.isManageReplicationRequested()) {
            Map<String, ReplicationSafetyResult> blocked = new TreeMap<>();
            var policy = new ReplicationSafetyPolicy();
            var permissions = new ReplicationSafetyPolicy.Permissions(false,
                    managerConfig.isAllowReplicationDecreaseRequested(), managerConfig.isDeleteDisabled());
            plan.getTopicPlans().forEach(t -> t.getReplicationPlan().ifPresent(operation -> blocked.put(t.getName(),
                    policy.blockedBeforeMetadata(operation, policy.permissions(operation, permissions)))));
            throw new ReplicationPreflightException(blocked);
        }
    }

    private void applyWithReplication(DesiredPlan plan, ApplyReport report) {
        validatePermissions();
        PlanValidation.validate(plan);
        var permissions = new ReplicationSafetyPolicy.Permissions(managerConfig.isManageReplicationRequested(),
                managerConfig.isAllowReplicationDecreaseRequested(), managerConfig.isDeleteDisabled());
        var policy = new ReplicationSafetyPolicy();
        List<TopicPlan> topics = plan.getTopicPlans().stream().filter(t -> t.getReplicationPlan().isPresent()).toList();
        if (topics.stream().anyMatch(t -> !policy.permissions(t.getReplicationPlan().orElseThrow(), permissions).isEmpty())) {
            Map<String, ReplicationSafetyResult> results = new TreeMap<>();
            topics.forEach(t -> results.put(t.getName(), policy.blockedBeforeMetadata(t.getReplicationPlan().orElseThrow(),
                    policy.permissions(t.getReplicationPlan().orElseThrow(), permissions))));
            throw new ReplicationPreflightException(results);
        }
        // Other partition prerequisites must also pass before RF writes can start.
        preflightPartitions(plan, report);
        ReassignmentExecutor.ExecutionResult result = replicationApplication.apply(topics, permissions);
        if (result.state() != ReassignmentExecutor.State.COMPLETED) {
            throw new ReplicationExecutionException(result, ReplicationExecutionException.Phase.EXECUTION);
        }
        if (report != null) report.recordReplication(result, true, true);
        try {
            applyTopicOperations(plan, true, report);
            if (!managerConfig.isSkipAclsDisabled()) applyAclOperations(plan, report);
        } catch (RuntimeException ex) {
            var stale = ex instanceof KafkaExecutionException kafka
                    && kafka.getCode() == KafkaExecutionException.Code.PARTITION_PLAN_STALE ? kafka : null;
            throw new ReplicationExecutionException(result, ReplicationExecutionException.Phase.FOLLOWUP, stale);
        }
    }

    public void applyTopics(DesiredPlan desiredPlan) {
        applyTopics(desiredPlan, null);
    }

    private void applyTopics(DesiredPlan desiredPlan, ApplyReport report) {
        validatePermissions();
        if (managerConfig.isSkipTopicsDisabled()) {
            return;
        }
        PlanValidation.validate(desiredPlan);
        preflightReplication(desiredPlan);
        preflightPartitions(desiredPlan, report);
        applyTopicOperations(desiredPlan, false, report);
    }

    private void preflightPartitions(DesiredPlan desiredPlan, ApplyReport report) {
        // Check all partition preconditions before any topic is created, deleted or reconfigured.
        List<String> names = desiredPlan.getTopicPlans().stream().filter(t -> t.getPartitionPlan().isPresent())
                .map(TopicPlan::getName).toList();
        Map<String, TopicDescription> descriptions;
        try {
            descriptions = kafkaService.describeTopics(names);
        } catch (KafkaExecutionException ex) {
            if (report != null) report.blockPartitionPreflight(null);
            throw ex;
        }
        desiredPlan.getTopicPlans().forEach(topic -> topic.getPartitionPlan().ifPresent(partitions -> {
            TopicDescription current = descriptions.get(topic.getName());
            if (current == null || current.partitions().isEmpty()) {
                if (report != null) report.blockPartitionPreflight(topic.getName());
                throw new KafkaExecutionException("Partition preflight failed.", "Missing metadata for topic " + topic.getName() + ". Re-plan before retrying.");
            }
            int count = current.partitions().size();
            if (count != partitions.getPreviousCount() && count != partitions.getTargetCount()) {
                if (report != null) report.blockPartitionPreflight(topic.getName());
                throw new KafkaExecutionException("Partition plan is stale.", "Topic " + topic.getName() + " has "
                        + count + " partitions; expected " + partitions.getPreviousCount() + " or already "
                        + partitions.getTargetCount() + ". Re-plan before retrying.",
                        KafkaExecutionException.Code.PARTITION_PLAN_STALE, KafkaExecutionException.WriteOutcome.NOT_SENT);
            }
            topic.getTopicDetails().ifPresent(details -> LogUtil.printReplicationWarning(topic.getName(), details, current));
        }));
    }

    private void applyTopicOperations(DesiredPlan desiredPlan, boolean replicationCompleted, ApplyReport report) {
        for (TopicPlan topicPlan : desiredPlan.getTopicPlans()) {
            var action = topicPlan.getAction();
            if (action == PlanAction.NO_CHANGE || (action == PlanAction.REMOVE && managerConfig.isDeleteDisabled())) continue;
            var change = report == null ? null : report.topic(topicPlan.getName());
            if (change == null) LogUtil.printTopicPreApply(topicPlan, replicationCompleted && topicPlan.getReplicationPlan().isPresent());

            if (action == PlanAction.ADD) {
                execute(change == null ? null : change.lifecycle(),
                        () -> kafkaService.createTopic(topicPlan.getName(), topicPlan.getTopicDetails().orElseThrow()));
            } else if (action == PlanAction.UPDATE) {
                topicPlan.getPartitionPlan().ifPresent(partitions -> {
                    // Re-check immediately before the write; the service also verifies the resulting metadata.
                    boolean changed = execute(change == null ? null : change.partitions(),
                            () -> kafkaService.increaseTopicPartitions(topicPlan.getName(), partitions.getPreviousCount(), partitions.getTargetCount()));
                    if (!changed && change == null) {
                        System.out.printf("Topic %s already has %s partitions; partition change is a no-op.%n",
                                topicPlan.getName(), partitions.getTargetCount());
                    }
                });
                int index = 0;
                for (TopicConfigPlan config : topicPlan.getTopicConfigPlans()) {
                    if (config.getAction() == PlanAction.NO_CHANGE) continue;
                    execute(change == null ? null : change.configs().get(index++).step(), () -> applyTopicConfiguration(topicPlan, config));
                }
            } else if (action == PlanAction.REMOVE) {
                execute(change == null ? null : change.lifecycle(), () -> kafkaService.deleteTopic(topicPlan.getName()));
            }
            if (change == null) LogUtil.printPostApply();
        }
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
        applyAcls(desiredPlan, null);
    }

    private void applyAcls(DesiredPlan desiredPlan, ApplyReport report) {
        validatePermissions();
        if (desiredPlan.getTopicPlans().stream().anyMatch(t -> t.getReplicationPlan().isPresent())
                && desiredPlan.getAclPlans().stream().anyMatch(a -> a.getAction() == PlanAction.ADD
                || (a.getAction() == PlanAction.REMOVE && !managerConfig.isDeleteDisabled()))) {
            PlanValidation.validate(desiredPlan);
            preflightReplication(desiredPlan);
        }
        applyAclOperations(desiredPlan, report);
    }

    private void applyAclOperations(DesiredPlan desiredPlan, ApplyReport report) {
        int index = 0;
        for (var acl : desiredPlan.getAclPlans()) {
            var action = acl.getAction();
            if (action != PlanAction.ADD && (action != PlanAction.REMOVE || managerConfig.isDeleteDisabled())) continue;
            Step step = report == null ? null : report.acls().get(index++).step();
            if (step == null) LogUtil.printAclPreApply(acl);
            if (action == PlanAction.ADD) {
                execute(step, () -> kafkaService.createAcl(acl.getAclDetails().toAclBinding()));
            } else {
                boolean deleted = execute(step, () -> kafkaService.deleteAcl(acl.getAclDetails().toAclBinding()));
                if (!deleted && step == null) {
                    System.out.println("ACL already absent; no change.");
                    continue;
                }
            }
            if (step == null) LogUtil.printPostApply();
        }
    }

    private static void execute(Step step, Runnable action) {
        execute(step, () -> { action.run(); return true; });
    }

    private static boolean execute(Step step, BooleanSupplier action) {
        try {
            boolean changed = action.getAsBoolean();
            if (step != null) step.record(changed ? SUCCESS : NO_OP);
            return changed;
        } catch (RuntimeException error) {
            if (step != null) step.record(error instanceof KafkaExecutionException kafka ? switch (kafka.getWriteOutcome()) {
                case CONFIRMED -> SUCCESS;
                case NO_OP -> NO_OP;
                case REJECTED -> FAILED;
                case NOT_SENT -> BLOCKED;
                case PARTIAL -> PARTIAL;
                case UNKNOWN -> UNKNOWN;
            } : UNKNOWN);
            throw error;
        }
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
