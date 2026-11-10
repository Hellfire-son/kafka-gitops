package com.devshawn.kafka.gitops.manager;

import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.AclPlan;
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.PlanOverview;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPartitionPlan;
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
import com.devshawn.kafka.gitops.util.PlanUtil;
import com.devshawn.kafka.gitops.util.PlanValidation;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation.InvalidReplicationPlanException;
import com.devshawn.kafka.gitops.util.TopicConfigSecurity;
import com.devshawn.kafka.gitops.util.LogUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
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

public class PlanManager {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PlanManager.class);

    private final ManagerConfig managerConfig;
    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    public PlanManager(ManagerConfig managerConfig, KafkaService kafkaService, ObjectMapper objectMapper) {
        this.managerConfig = managerConfig;
        this.kafkaService = kafkaService;
        // Plan files are executable input. Keep strict reading local to plans, not the shared mapper.
        this.objectMapper = objectMapper.copy()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
                .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .disable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
                .disable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        this.objectMapper.coercionConfigFor(LogicalType.Integer).setCoercion(CoercionInputShape.String, CoercionAction.Fail);
    }

    public void planTopics(DesiredState desiredState, DesiredPlan.Builder desiredPlan) {
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
        Map<String, List<ConfigEntry>> topicConfigs = fetchTopicConfigurations(topicNames);

        desiredState.getTopics().forEach((key, value) -> {
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
                LogUtil.printReplicationWarning(key, value, description);
                planTopicConfigurations(key, value, topicConfigs.get(key), topicPlan);
            }

            desiredPlan.addTopicPlans(topicPlan.build());
        });

        for (TopicListing currentTopic : topics) {
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

        configPlans.forEach((key, plan) -> {
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
            } catch (FileNotFoundException ex) {
                throw new ReadPlanInputException("The specified plan file could not be found.");
            } catch (InvalidReplicationPlanException ex) {
                throw new ReadPlanInputException("Invalid replication plan: " + ex.getMessage());
            } catch (JsonMappingException ex) {
                if (ex.getPath().stream().anyMatch(reference -> "replicationPlan".equals(reference.getFieldName()))) {
                    throw new ReadPlanInputException("Invalid replicationPlan structure, field type, required field or operation. Only schemaVersion 1 and UPDATE are supported.");
                }
                throw new ReadPlanInputException();
            } catch (IOException ex) {
                throw new ReadPlanInputException();
            } catch (IllegalArgumentException ex) {
                throw new ReadPlanInputException("Unsupported or incomplete operations in the plan. Generate a new plan.");
            }
        }).orElse(null);
    }

    public void writePlanToFile(DesiredPlan desiredPlan) {
        managerConfig.getPlanFile().ifPresent(planFile -> {
            try {
                DesiredPlan outputPlan = managerConfig.isIncludeUnchangedEnabled() ? desiredPlan : desiredPlan.toChangesOnlyPlan();
                // Serialize before opening the destination: confidential values must not truncate an existing file.
                String output = objectMapper.writeValueAsString(outputPlan);
                if (!planFile.createNewFile()) {
                    LOG.info("Overwriting existing plan file at {}", planFile);
                }
                try(FileWriter writer = new FileWriter(planFile)) {
                    writer.write(output);
                }
            } catch (InvalidReplicationPlanException ex) {
                throw new WritePlanOutputException("Invalid replication plan: " + ex.getMessage());
            } catch (JsonProcessingException ex) {
                throw new WritePlanOutputException("Plan serialization refused: invalid structure or confidential values. Confidential values cannot be exported; apply directly from the state file.");
            } catch (IOException ex) {
                throw new WritePlanOutputException(ex.getMessage());
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
