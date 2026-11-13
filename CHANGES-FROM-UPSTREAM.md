<!-- SPDX-License-Identifier: Apache-2.0 -->
# Provenance and modifications

Independent community-maintained fork of joschi/kafka-gitops 0.3.1
(a16eb39c45e609d19ca2b12ff6def3e22a46dffc), originally created by Shawn Seymour
as devshawn/kafka-gitops. Copyright 2020 Shawn Seymour and the original
Apache License 2.0 are retained. Packages remain com.devshawn.*.
No endorsement or official support by ASF, Arenadata, Confluent or the original
maintainers is claimed. No new personal name or organization is asserted.

New original contributions in this fork are distributed under Apache-2.0;
third-party components retain their own licenses. See LICENSE, CHANGELOG.md,
legal/README.md and legal/THIRD-PARTY-NOTICES.

Changes from upstream include Java/dependency modernization, configuration
precedence/redaction, topic configuration, partition increases, RF planning and
execution safety, ELR evidence handling and regression tests. Release preparation
changes version metadata/legal packaging, docs/CI and runner path portability;
it does not change the RF algorithm or ACL/DESCRIBE behavior.

## File-level notices

Changed upstream source/config/docs files carry a visible modification comment.
Machine-readable JSON, exact-output TXT fixtures and the official wrapper binary
have an adjacent <filename>.NOTICE instead, preserving parser/test/binary bytes.
These sidecars belong with the respective source files. Removed workflows are
listed below; their provenance remains in Git history.

| Upstream file | Status | Modification summary |
|---|---|---|
| `.github/workflows/dependency-review.yml` | D | Removed publishing automation or replaced CI with safe local build/test steps. |
| `.github/workflows/gradle.yml` | M | Removed publishing automation or replaced CI with safe local build/test steps. |
| `.github/workflows/release.yml` | D | Removed publishing automation or replaced CI with safe local build/test steps. |
| `.github/workflows/static.yml` | D | Removed publishing automation or replaced CI with safe local build/test steps. |
| `.gitignore` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `CONTRIBUTING.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `README.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `build.gradle` | M | Java/dependency modernization, reproducible release packaging and verification. |
| `docs/_coverpage.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/_sidebar.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/audit-report-2026-11-09.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/audit-report-stage4-2026-11-09.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/dependencies.csv` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/dependency-inventory.json` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/gradle-jars.csv` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/protocol-ranges.json` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/sca-results.json` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/audit/modernization-stage1-2026-11-11/verification.json` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/development-task2-compatibility-rf-ux.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/elr-replication-compatibility.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6-replication.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6b2-planning.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6b3a-preflight.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6b3b-executor.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6b3c-integration.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/design/stage6b4-regression.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/documentation.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/index.html` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/installation.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/quick-start.md` | M | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/testing/development-task3-final-qa.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/testing/kafka-3x-compatibility-results.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/testing/kafka-4.4-docker-integration-results.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/testing/kafka-4.4-elr-rf-results.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `docs/testing/kafka-integration-test-plan.md` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `gradle/wrapper/gradle-wrapper.jar` | M | Updated to the official Gradle 9.9.1 wrapper; original wrapper copyright is retained. |
| `gradle/wrapper/gradle-wrapper.properties` | M | Updated to the official Gradle 9.9.1 wrapper; original wrapper copyright is retained. |
| `gradlew` | M | Updated to the official Gradle 9.9.1 wrapper; original wrapper copyright is retained. |
| `gradlew.bat` | M | Updated to the official Gradle 9.9.1 wrapper; original wrapper copyright is retained. |
| `settings.gradle` | M | Java/dependency modernization, reproducible release packaging and verification. |
| `src/main/java/com/devshawn/kafka/gitops/MainCommand.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/StateManager.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/cli/AccountCommand.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/cli/ApplyCommand.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/cli/PlanCommand.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/cli/ValidateCommand.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/config/KafkaGitopsConfig.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoader.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/config/ManagerConfig.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/confluent/ServiceAccount.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/metadata/CapabilityAssessment.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/metadata/ReplicationClusterSnapshot.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/metadata/ReplicationSafetyEvidence.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/options/GetAclOptions.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/AclPlan.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/BrokerSnapshot.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/DesiredPlan.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/PartitionReplicaAssignment.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/PlanOverview.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/PlanningReport.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/ReplicationSafetyResult.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicConfigPlan.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicPartitionPlan.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicPlan.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicReplicationPlan.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicConfigPlanSerializer.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicPlanSerializer.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicReplicationPlanSerializer.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/AclDetails.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/CustomAclDetails.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/DesiredState.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/DesiredStateFile.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/TopicDetails.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/UserDetails.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/service/ApplicationService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectStorageTopics.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectorDetails.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaStreamsService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/Settings.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsCCloud.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsFiles.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsServices.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsServicesAcls.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopics.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopicsDefaults.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopicsList.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/enums/RackAwarenessPolicy.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/exception/KafkaExecutionException.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/exception/PlanBlockedException.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/exception/ReplicationExecutionException.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/exception/ReplicationPreflightException.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/manager/ApplyManager.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/manager/PlanManager.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/AdminReassignmentExecutor.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/AdminReplicationApplication.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ConfluentCloudService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ElrSafetyPolicy.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/KafkaCapabilities.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/KafkaService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ParserService.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ReassignmentExecutor.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ReplicaAssignmentPlanner.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ReplicationApplication.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ReplicationMetadataReader.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/service/ReplicationSafetyPolicy.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/KafkaDiagnostics.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/LogUtil.java` | M | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/PlanReportRenderer.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/PlanValidation.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/ReplicationPlanValidation.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/main/java/com/devshawn/kafka/gitops/util/TopicConfigSecurity.java` | A | Configuration, topic/partition/RF safety, compatibility, diagnostics or CLI modernization; see CHANGELOG.md. |
| `src/test/groovy/com/devshawn/kafka/gitops/DevelopmentTask2WorkflowSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/FinalQaRegressionSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/PartitionWorkflowSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/PlanTestSupport.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationApplicationWorkflowSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationPlanningSupport.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationSafetyTestSupport.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationTestSupport.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationWorkflowSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoaderSpec.groovy` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoggingSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/domain/state/CustomAclDetailsSpec.groovy` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ApplyManagerSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PartitionApplySpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PartitionJsonSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PartitionPlanSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanFixturesSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanJsonSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanManagerSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationApplyGuardSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationJsonSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationModelSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationPlanningSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationPreflightSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ConfluentCloudServiceSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ElrApplicationSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ElrSafetyPolicySpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/FinalQaProcessSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/FinalQaResourceSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/Kafka44RfOptionsSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/KafkaCompatibilityTask2Spec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/KafkaConnectionTask2Spec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/KafkaDiagnosticsAuditSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/KafkaPartitionServiceSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/LegacyFeatureEpochSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReassignmentExecutionPolicySpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReassignmentExecutorContractSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReassignmentExecutorSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicaAssignmentPlannerSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationApplicationTestHarness.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationFinalMetadataConvergenceSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationMetadataSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationPreflightMetadataSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationRegressionAuditSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationSafetyPolicySpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/groovy/com/devshawn/kafka/gitops/util/LogUtilSpec.groovy` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/java/com/devshawn/kafka/gitops/CompressionCompatibilityTest.java` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/java/com/devshawn/kafka/gitops/service/QaProcessProbe.java` | A | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/invalid-format-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/invalid-storage-topics-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/invalid-topic-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-acl-exists-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-acl-exists-plan.json` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-basic-include-unchanged-plan.json` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-basic-plan.json` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-2-plan.json` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-3-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-3-no-delete-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-3-plan.json` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/seed-topic-modification-no-delete-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/skip-acls-apply-apply-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `src/test/resources/plans/unrecognized-property-output.txt` | M | Updated fixtures/regressions for the independent fork; executable fixture contents retain their original format. |
| `tools/integration/Kafka3xDataProbe.java` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/Kafka3xLabProbe.java` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/Kafka3xTraceApply.java` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/KafkaElrProbe.java` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/KafkaLabProbe.java` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/kafka3x_lab.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/kafka44_compat_checks.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/kafka44_elr_rf.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/kafka44_lab.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/Dockerfile` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/broker-1.properties` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/broker-2.properties` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/broker-3.properties` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/compose.yaml` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/323/zookeeper.properties` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/legacy/371/compose.yaml` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/test_kafka3x_lab.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/test_kafka44_elr_rf.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
| `tools/integration/test_kafka44_lab.py` | A | Updated independent-fork documentation, configuration or publication metadata. |
