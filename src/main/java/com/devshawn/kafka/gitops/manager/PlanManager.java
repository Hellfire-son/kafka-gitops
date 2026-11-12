package com.devshawn.kafka.gitops.manager;

import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.AclPlan;
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.PlanOverview;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPartitionPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport;
import com.devshawn.kafka.gitops.exception.PlanBlockedException;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.state.AclDetails;
import com.devshawn.kafka.gitops.domain.state.DesiredState;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.exception.PlanIsUpToDateException;
import com.devshawn.kafka.gitops.exception.ReadPlanInputException;
import com.devshawn.kafka.gitops.exception.WritePlanOutputException;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import com.devshawn.kafka.gitops.service.KafkaService;
import com.devshawn.kafka.gitops.service.ReplicaAssignmentPlanner;
import com.devshawn.kafka.gitops.util.PlanUtil;
import com.devshawn.kafka.gitops.util.PlanValidation;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation.InvalidReplicationPlanException;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import com.devshawn.kafka.gitops.util.LogUtil;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.cfg.EnumFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.type.LogicalType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Comparator;

public class PlanManager {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PlanManager.class);

    private final ManagerConfig managerConfig;
    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;
    private boolean diagnosticMode;
    private final Map<String, PlanningReport.TopicPreview> previews = new TreeMap<>();
    private final List<PlanningReport.Issue> planningIssues = new ArrayList<>();

    public PlanningReport report(DesiredPlan changes) { return new PlanningReport(changes, previews, planningIssues); }
    public void beginReport() { diagnosticMode = true; previews.clear(); planningIssues.clear(); }

    public void planTopicsReported(DesiredState desired, DesiredPlan.Builder changes) {
        diagnosticMode = true;
        desired.getTopics().forEach((name, details) -> previews.put(name,
                new PlanningReport.TopicPreview(details, false, false, java.util.Optional.empty(), Map.of(), Map.of())));
        try { planTopics(desired, changes); }
        catch (KafkaExecutionException ex) { problem(ex.getCode().name(), "topics", "UNKNOWN", "readable metadata", "UNKNOWN", ex.getExceptionMessage()); }
        catch (ValidationException ex) { problem("TOPIC_PLANNING_BLOCKED", "topics", "INVALID", "valid topic changes", "UNSAFE", ex.getMessage()); }
    }

    public void planAclsReported(DesiredState desired, DesiredPlan.Builder changes) {
        try { planAcls(desired, changes); }
        catch (KafkaExecutionException ex) { problem(ex.getCode().name(), "ACLs", "UNKNOWN", "readable ACL metadata", "UNKNOWN", ex.getExceptionMessage()); }
    }

    private void problem(String code, String resource, String actual, String required, String state, String reason) {
        planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.BLOCKED, code, resource, actual, required, state,
                reason, "Resolve this condition and re-plan; no writes were sent."));
    }

    public PlanManager(ManagerConfig managerConfig, KafkaService kafkaService, ObjectMapper objectMapper) {
        this.managerConfig = managerConfig;
        this.kafkaService = kafkaService;
        // Plan files are executable input. Keep strict reading local to plans, not the shared mapper.
        this.objectMapper = objectMapper.rebuild()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(EnumFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .disable(EnumFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
                .disable(EnumFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE)
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .withCoercionConfig(LogicalType.Integer, cfg -> cfg.setCoercion(CoercionInputShape.String, CoercionAction.Fail))
                .build();
    }

    public void planTopics(DesiredState desiredState, DesiredPlan.Builder desiredPlan) {
        if (managerConfig.isAllowReplicationDecreaseRequested() && !managerConfig.isManageReplicationRequested()) {
            throw new ValidationException("--allow-replication-decrease requires --manage-replication.");
        }
        if (managerConfig.isSkipTopicsDisabled()) {
            return;
        }
        desiredState.getTopics().forEach((name, details) -> {
            if (details.getPartitions() < 1) {
                throw new ValidationException("The partition count must be a positive integer (topic: " + name + ").");
            }
        });
        List<TopicListing> topics = kafkaService.getTopics();
        List<String> topicNames = topics.stream().map(TopicListing::name).toList();
        List<String> managedTopics = desiredState.getTopics().keySet().stream().filter(topicNames::contains).toList();
        Map<String, TopicDescription> descriptions = kafkaService.describeTopics(managedTopics);
        if (diagnosticMode) desiredState.getTopics().forEach((name, details) -> previews.put(name,
                new PlanningReport.TopicPreview(details, true, topicNames.contains(name), java.util.Optional.ofNullable(descriptions.get(name)), Map.of(), Map.of())));
        Map<String, List<ConfigEntry>> topicConfigs = fetchTopicConfigurations(topicNames);
        if (diagnosticMode) previews.replaceAll((name, preview) -> {
            Map<String, String> values = new TreeMap<>();
            topicConfigs.getOrDefault(name, List.of()).forEach(c -> {
                if (!c.isSensitive() && !TopicConfigSecurity.isSensitive(c.name()) && c.value() != null) values.put(c.name(), c.value());
            });
            return new PlanningReport.TopicPreview(preview.desired(), preview.existenceKnown(), preview.exists(), preview.current(), preview.brokers(), values);
        });
        Map<String, TopicReplicationPlan> replications = managerConfig.isManageReplicationRequested()
                ? diagnosticMode ? planReportedReplications(desiredState, descriptions) : planReplications(desiredState, descriptions) : Map.of();

        Map<String, TopicDetails> orderedTopics = managerConfig.isManageReplicationRequested()
                ? new TreeMap<>(desiredState.getTopics()) : desiredState.getTopics();
        orderedTopics.forEach((key, value) -> {
            try {
            TopicPlan.Builder topicPlan = new TopicPlan.Builder()
                    .setName(key)
                    .setTopicDetails(value);

            if (!topicNames.contains(key)) {
                LOG.info("[PLAN] Topic {} does not exist; it will be created.", key);
                topicPlan.setAction(PlanAction.ADD);
            } else {
                LOG.info("[PLAN] Topic {} exists, it will not be created.", key);
                topicPlan.setAction(PlanAction.NO_CHANGE);
                TopicDescription description = descriptions.get(key);
                if (description == null || description.partitions().isEmpty()) {
                    throw new KafkaExecutionException("Cannot plan topic partitions.", "Missing partition metadata for topic " + key + ".");
                }
                int currentCount = description.partitions().size();
                if (value.getPartitions() < currentCount) {
                    throw new ValidationException("Decreasing partitions is not supported (topic: " + key + ", "
                            + currentCount + " -> " + value.getPartitions() + ").");
                }
                if (value.getPartitions() > currentCount) {
                    topicPlan.setPartitionPlan(new TopicPartitionPlan.Builder().setAction(PlanAction.UPDATE)
                            .setPreviousCount(currentCount).setTargetCount(value.getPartitions()).build());
                    topicPlan.setAction(PlanAction.UPDATE);
                }
                if (replications.containsKey(key)) {
                    topicPlan.setReplicationPlan(replications.get(key)).setAction(PlanAction.UPDATE);
                } else if (!managerConfig.isManageReplicationRequested() && !diagnosticMode) {
                    LogUtil.printReplicationWarning(key, value, description);
                }
                if (diagnosticMode && !managerConfig.isManageReplicationRequested() && value.getReplication().isPresent()
                        && description.partitions().stream().anyMatch(p -> p.replicas().size() != value.getReplication().get())) {
                    planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.WARNING, "RF_MANAGEMENT_DISABLED", key,
                            description.partitions().stream().map(p -> p.replicas().size()).distinct().toList().toString(),
                            value.getReplication().get().toString(), "NOT_MANAGED", "RF was explicitly excluded by --no-manage-replication.",
                            "Remove --no-manage-replication to include RF in the plan."));
                }
                planTopicConfigurations(key, value, topicConfigs.get(key), topicPlan);
                if (replications.containsKey(key)) {
                    TopicPlan built = topicPlan.build();
                    if (built.getTopicConfigPlans().stream().anyMatch(config -> config.getAction() != PlanAction.NO_CHANGE
                            && (config.getKey().equals("min.insync.replicas") || config.getKey().contains("throttled")))) {
                        throw new ValidationException("RF changes cannot be combined with min.insync.replicas or throttle configuration changes. Use separate plans.");
                    }
                }
            }

            desiredPlan.addTopicPlans(topicPlan.build());
            } catch (ValidationException ex) {
                if (!diagnosticMode) throw ex;
                problem("TOPIC_CHANGE_UNSAFE", key, "INVALID", "compatible changes", "UNSAFE", ex.getMessage());
            }
        });

        List<TopicListing> orderedCurrent = managerConfig.isManageReplicationRequested()
                ? topics.stream().sorted(Comparator.comparing(TopicListing::name)).toList() : topics;
        for (TopicListing currentTopic : orderedCurrent) {
            boolean acceptTopic = desiredState.getPrefixedTopicsToAccept().stream().anyMatch(it -> currentTopic.name().startsWith(it));
            if (!desiredState.getPrefixedTopicsToAccept().isEmpty() && !acceptTopic) {
                LOG.info("[PLAN] Ignoring non-included topic {} due to prefix", currentTopic.name());
                continue;
            }

            boolean ignoreTopic = desiredState.getPrefixedTopicsToIgnore().stream().anyMatch(it -> currentTopic.name().startsWith(it));
            if (ignoreTopic) {
                LOG.info("[PLAN] Ignoring excluded topic {} due to prefix", currentTopic.name());
                continue;
            }

            if (!managerConfig.isDeleteDisabled() && !desiredState.getTopics().containsKey(currentTopic.name())) {
                TopicPlan topicPlan = new TopicPlan.Builder()
                        .setName(currentTopic.name())
                        .setAction(PlanAction.REMOVE)
                        .build();

                desiredPlan.addTopicPlans(topicPlan);
            }
        }
        if (managerConfig.isManageReplicationRequested()) {
            PlanValidation.validate(desiredPlan.build());
        }
    }

    private Map<String, TopicReplicationPlan> planReportedReplications(DesiredState desired, Map<String, TopicDescription> descriptions) {
        Map<String, Integer> targets = new TreeMap<>();
        desired.getTopics().forEach((name, details) -> {
            TopicDescription current = descriptions.get(name);
            if (current != null && details.getReplication().isPresent()
                    && current.partitions().stream().anyMatch(p -> p.replicas().size() != details.getReplication().get())) {
                if (details.getPartitions() != current.partitions().size()) {
                    problem("PARTITION_RF_CONFLICT", name, current.partitions().size() + " partitions", Integer.toString(details.getPartitions()),
                            "UNSAFE", "Plan RF and partition count changes separately.");
                } else targets.put(name, details.getReplication().get());
            }
        });
        if (targets.isEmpty()) return Map.of();
        ReplicationClusterSnapshot snapshot;
        try { snapshot = kafkaService.readReplicationSnapshot(targets.keySet()); }
        catch (KafkaExecutionException ex) {
            targets.forEach((name, rf) -> problem(ex.getCode().name(), name, "RF metadata UNKNOWN", "RF " + rf,
                    "UNKNOWN", ex.getExceptionMessage()));
            return Map.of();
        }
        Map<String, TopicReplicationPlan> plans = new TreeMap<>();
        Map<Integer, Long> projectedLoad = new TreeMap<>(snapshot.replicaCounts());
        for (var target : targets.entrySet()) {
            String name = target.getKey();
            int rf = target.getValue();
            var preview = previews.get(name);
            previews.put(name, new PlanningReport.TopicPreview(preview.desired(), true, true, preview.current(), snapshot.brokers(), preview.currentConfigValues()));
            var before = descriptions.get(name);
            var topic = snapshot.topics().get(name);
            if (topic == null || !before.topicId().toString().equals(topic.topicId()) || before.partitions().size() != topic.partitions().size()
                    || before.partitions().stream().anyMatch(p -> topic.partitions().stream().noneMatch(a -> a.id() == p.partition()
                    && a.replicas().equals(p.replicas().stream().map(n -> n.id()).toList())))) {
                problem("TOPIC_METADATA_CHANGED", name, "changed identity/assignments", "stable initial metadata", "UNSAFE", "Metadata changed during planning.");
                continue;
            }
            snapshot.brokers().forEach((id, broker) -> broker.capabilities().forEach((capability, assessment) -> {
                if (assessment.state() == com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.State.UNKNOWN && !assessment.critical()) {
                    planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.WARNING, assessment.reason().name(), name + "/broker-" + id,
                            "UNKNOWN", capability.name(), assessment.state().name(), "This additional operational risk is not established by the available Admin evidence.",
                            "Review broker capacity/admission policy; apply still rechecks all critical invariants."));
                }
            }));
            var elr = snapshot.safety().elrMode();
            if (elr == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
                    || elr == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED) {
                problem(elr == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
                                ? "ELR_MODE_UNKNOWN" : "ELR_UNSUPPORTED", name, elr.name(), "ELR DISABLED or positively NOT_APPLICABLE",
                        elr.name(), "RF execution requires a known ELR mode; the current assignment algorithm does not support enabled ELR.");
                // Keep broker diagnostics and other known changes, without inventing executable assignments.
                continue;
            }
            try {
                ReplicationClusterSnapshot projected = new ReplicationClusterSnapshot(snapshot.clusterId(), snapshot.brokers(), snapshot.topics(),
                        snapshot.reassignments(), snapshot.reassignmentState(), projectedLoad, snapshot.loadState(), snapshot.safety());
                // Preview never carries execution permissions. Decreases are visible even before apply approval.
                var plan = new ReplicaAssignmentPlanner().plan(projected, Map.of(name, rf), managerConfig.getReplicationRackPolicy(), true, false).get(name);
                if (plan != null) {
                    var excludedTargets = plan.getAssignments().stream().flatMap(a -> a.getTargetReplicas().stream()).distinct().sorted()
                            .filter(id -> snapshot.brokers().get(id) == null || !snapshot.brokers().get(id).isPreviewCandidate()
                                    || !snapshot.brokers().get(id).fencingEstablished()).toList();
                    if (!excludedTargets.isEmpty()) {
                        problem("TARGET_BROKER_INELIGIBLE", name, "Excluded target brokers=" + excludedTargets,
                                "all target brokers have established core eligibility", "UNSAFE_OR_UNKNOWN",
                                "Preserving an existing replica does not bypass target safety. Inspect the broker observations above.");
                        continue;
                    }
                    plans.put(name, plan);
                    if (plan.getRackPolicy() == com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.BROKER_ONLY)
                        planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.WARNING, "BROKER_ONLY", name,
                                "rack policy disabled", "review rack failure tolerance", "OPERATIONAL_RISK",
                                "No rack failure protection is established.", "Use RACK_AWARE when reliable rack metadata is available."));
                    if (snapshot.brokers().values().stream().anyMatch(b -> !b.capabilities().isEmpty() && b.minimumUsableBytes().isPresent()))
                        planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.WARNING, "DISK_RESERVE_NOT_PROVEN", name,
                                "transfer volume/growth/concurrent placement UNKNOWN", "sufficient disk reserve", "UNKNOWN",
                                "Positive usableBytes does not prove sufficient capacity for the proposed copy.", "Review disk headroom before apply."));
                    plan.getAssignments().forEach(a -> {
                        a.getPreviousReplicas().forEach(id -> projectedLoad.merge(id, -1L, Long::sum));
                        a.getTargetReplicas().forEach(id -> projectedLoad.merge(id, 1L, Long::sum));
                    });
                    if (plan.requiresDecreaseApproval()) planningIssues.add(new PlanningReport.Issue(PlanningReport.Severity.WARNING,
                            managerConfig.isDeleteDisabled() ? "NO_DELETE" : "DECREASE_PERMISSION_REQUIRED", name,
                            "RF decrease proposed", "apply --allow-replication-decrease without --no-delete", "APPLY_PERMISSION_REQUIRED",
                            "Planning is read-only; these permissions are checked independently at apply.", "Review the decrease before applying."));
                }
            } catch (ValidationException ex) {
                long eligible = snapshot.brokers().values().stream().filter(b -> b.isPreviewCandidate() && b.fencingEstablished()).count();
                problem(eligible < rf ? "INSUFFICIENT_BROKERS" : "RF_PLANNING_BLOCKED", name,
                        "Discovered=" + snapshot.brokers().size() + ", eligible=" + eligible,
                        "RF=" + rf, "BLOCKED", ex.getMessage());
            }
        }
        return plans;
    }

    private Map<String, TopicReplicationPlan> planReplications(DesiredState desiredState,
                                                             Map<String, TopicDescription> descriptions) {
        Map<String, Integer> targets = new TreeMap<>();
        desiredState.getTopics().forEach((name, details) -> {
            TopicDescription current = descriptions.get(name);
            if (current != null && details.getReplication().isPresent()) {
                int target = details.getReplication().get();
                if (target <= 0 || target > Short.MAX_VALUE) {
                    throw new ValidationException("The target RF must be in 1..32767.");
                }
                if (current.partitions().stream().anyMatch(p -> p.replicas().size() != target)) {
                    if (details.getPartitions() != current.partitions().size()) {
                        throw new ValidationException("partitionPlan and replicationPlan cannot be combined for one topic. Plan RF and partition changes separately.");
                    }
                    targets.put(name, target);
                }
            }
        });
        if (targets.isEmpty()) {
            return Map.of();
        }
        ReplicationClusterSnapshot snapshot = kafkaService.readReplicationSnapshot(targets.keySet());
        targets.forEach((name, rf) -> {
            TopicDescription before = descriptions.get(name);
            var after = snapshot.topics().get(name);
            if (after == null || !before.topicId().toString().equals(after.topicId())
                    || before.partitions().size() != after.partitions().size()
                    || before.partitions().stream().anyMatch(p -> after.partitions().stream().noneMatch(a -> a.id() == p.partition()
                    && a.replicas().equals(p.replicas().stream().map(node -> node.id()).toList())))) {
                throw new ValidationException("Topic metadata changed before RF planning. Re-plan.");
            }
        });
        Map<String, TopicReplicationPlan> plans = new ReplicaAssignmentPlanner().plan(snapshot, targets,
                managerConfig.getReplicationRackPolicy(), managerConfig.isAllowReplicationDecreaseRequested(), managerConfig.isDeleteDisabled());
        if (plans.values().stream().anyMatch(p -> p.getRackPolicy() == com.devshawn.kafka.gitops.enums.RackAwarenessPolicy.BROKER_ONLY)) {
            System.out.println("[WARNING] BROKER_ONLY disables rack awareness; rack failure protection is not established.");
        }
        System.out.println("[WARNING] RF planning preview only: replica counts do not prove disk capacity. Apply requires a fresh safety preflight.");
        return plans;
    }

    private void planTopicConfigurations(String topicName, TopicDetails topicDetails, List<ConfigEntry> configs, TopicPlan.Builder topicPlan) {
        Map<String, TopicConfigPlan> configPlans = new HashMap<>();
        List<ConfigEntry> currentConfigs = configs == null ? List.of() : configs;
        List<ConfigEntry> customConfigs = currentConfigs.stream()
                .filter(it -> it.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .toList();

        customConfigs.forEach(currentConfig -> {
            String newConfig = topicDetails.getConfigs().getOrDefault(currentConfig.name(), null);

            TopicConfigPlan.Builder topicConfigPlan = new TopicConfigPlan.Builder()
                    .setKey(currentConfig.name())
                    .setSensitive(currentConfig.isSensitive() || TopicConfigSecurity.isSensitive(currentConfig.name()));

            if (newConfig == null) {
                topicConfigPlan.setAction(PlanAction.REMOVE);
                if (!topicConfigPlan.isSensitive()) {
                    topicConfigPlan.setNullablePreviousValue(currentConfig.value());
                }
                configPlans.put(currentConfig.name(), topicConfigPlan.build());
                topicPlan.setAction(PlanAction.UPDATE);
            } else if (Objects.equals(currentConfig.value(), newConfig)) {
                topicConfigPlan.setAction(PlanAction.NO_CHANGE);
                if (!topicConfigPlan.isSensitive()) {
                    topicConfigPlan.setValue(newConfig);
                }
                configPlans.put(currentConfig.name(), topicConfigPlan.build());
            } else {
                topicConfigPlan.setAction(PlanAction.UPDATE).setValue(newConfig);
                if (!topicConfigPlan.isSensitive()) {
                    topicConfigPlan.setNullablePreviousValue(currentConfig.value());
                }
                configPlans.put(currentConfig.name(), topicConfigPlan.build());
                topicPlan.setAction(PlanAction.UPDATE);
            }
        });

        topicDetails.getConfigs().forEach((key, value) -> {
            if (!configPlans.containsKey(key) && value != null) {
                boolean sensitive = TopicConfigSecurity.isSensitive(key) || currentConfigs.stream()
                        .anyMatch(config -> config.name().equals(key) && config.isSensitive());
                TopicConfigPlan configPlan = new TopicConfigPlan.Builder()
                        .setKey(key).setValue(value).setSensitive(sensitive).setAction(PlanAction.ADD).build();
                configPlans.put(key, configPlan);
                topicPlan.setAction(PlanAction.UPDATE);
            }
        });

        Map<String, TopicConfigPlan> ordered = managerConfig.isManageReplicationRequested() ? new TreeMap<>(configPlans) : configPlans;
        ordered.forEach((key, plan) -> {
            LOG.info("[PLAN] Topic {} | [{}] {}", topicName, plan.getAction(), plan.getKey());
            topicPlan.addTopicConfigPlans(plan);
        });
    }

    public void planAcls(DesiredState desiredState, DesiredPlan.Builder desiredPlan) {
        List<AclBinding> currentAcls = kafkaService.getAcls();

        currentAcls.forEach(acl -> {
            Map.Entry<String, AclDetails> detailsEntry = desiredState.getAcls().entrySet().stream()
                    .filter(entry -> entry.getValue().equalsAclBinding(acl))
                    .findFirst().orElse(null);

            AclPlan.Builder aclPlan = new AclPlan.Builder();

            if (detailsEntry != null) {
                aclPlan.setName(detailsEntry.getKey());
                aclPlan.setAclDetails(detailsEntry.getValue());
                aclPlan.setAction(PlanAction.NO_CHANGE);
                desiredPlan.addAclPlans(aclPlan.build());
            } else {
                aclPlan.setName("Unnamed ACL");
                aclPlan.setAclDetails(AclDetails.fromAclBinding(acl));
                aclPlan.setAction(PlanAction.REMOVE);

                if (!managerConfig.isDeleteDisabled()) {
                    desiredPlan.addAclPlans(aclPlan.build());
                }
            }
        });

        desiredState.getAcls().forEach((key, value) -> {
            AclBinding aclBinding = currentAcls.stream().filter(value::equalsAclBinding).findFirst().orElse(null);
            if (aclBinding == null) {
                AclPlan aclPlan = new AclPlan.Builder()
                        .setName(key)
                        .setAclDetails(value)
                        .setAction(PlanAction.ADD)
                        .build();

                desiredPlan.addAclPlans(aclPlan);
            }
        });
    }

    public void validatePlanHasChanges(DesiredPlan desiredPlan,
                                       boolean deleteDisabled,
                                       boolean skipAclsDisabled,
                                       boolean skipTopicsDisabled) {
        PlanOverview planOverview = PlanUtil.getOverview(desiredPlan, deleteDisabled, skipAclsDisabled, skipTopicsDisabled);
        if (planOverview.getAdd() == 0 && planOverview.getUpdate() == 0 && planOverview.getRemove() == 0) {
            throw new PlanIsUpToDateException();
        }
    }

    public DesiredPlan readPlanFromFile() {
        return managerConfig.getPlanFile().map(file -> {
            try {
                DesiredPlan plan = objectMapper.readValue(file, DesiredPlan.class);
                PlanValidation.validate(plan);
                return plan;
            } catch (InvalidReplicationPlanException ex) {
                throw new ReadPlanInputException("Invalid replication plan: " + ex.getMessage());
            } catch (JacksonException ex) {
                if (ex.getPath().stream().anyMatch(reference -> "replicationPlan".equals(reference.getPropertyName()))) {
                    throw new ReadPlanInputException("Invalid replicationPlan structure, field type, required field or operation. Only schemaVersion 1 and UPDATE are supported.");
                }
                if (ex.getCause() instanceof FileNotFoundException) {
                    throw new ReadPlanInputException("The specified plan file could not be found.");
                }
                throw new ReadPlanInputException();
            } catch (IllegalArgumentException ex) {
                throw new ReadPlanInputException("Unsupported or incomplete operations in the plan. Generate a new plan.");
            }
        }).orElse(null);
    }

    public void writePlanToFile(DesiredPlan desiredPlan) {
        if (report(desiredPlan).blocked()) throw new PlanBlockedException(report(desiredPlan));
        managerConfig.getPlanFile().ifPresent(planFile -> {
            try {
                DesiredPlan outputPlan = managerConfig.isIncludeUnchangedEnabled() ? desiredPlan : desiredPlan.toChangesOnlyPlan();
                // Serialize before opening the destination: confidential values must not truncate an existing file.
                String output = objectMapper.writeValueAsString(outputPlan);
                if (!planFile.createNewFile()) {
                    LOG.info("Overwriting existing plan file.");
                }
                try(FileWriter writer = new FileWriter(planFile)) {
                    writer.write(output);
                }
            } catch (InvalidReplicationPlanException ex) {
                throw new WritePlanOutputException("Invalid replication plan: " + ex.getMessage());
            } catch (JacksonException ex) {
                throw new WritePlanOutputException("Plan serialization refused: invalid structure or confidential values. Confidential values cannot be exported; apply directly from the state file.");
            } catch (IOException ex) {
                throw new WritePlanOutputException("The execution plan file could not be written. Check file access.");
            }
        });
    }

    private Map<String, List<ConfigEntry>> fetchTopicConfigurations(List<String> topicNames) {
        Map<String, List<ConfigEntry>> map = new HashMap<>();
        Map<ConfigResource, Config> configs = kafkaService.describeConfigsForTopics(topicNames);
        configs.forEach((key, value) -> map.put(key.name(), new ArrayList<>(value.entries())));
        return map;
    }
}
