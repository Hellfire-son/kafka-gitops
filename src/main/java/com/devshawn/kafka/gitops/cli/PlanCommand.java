package com.devshawn.kafka.gitops.cli;

import com.devshawn.kafka.gitops.MainCommand;
import com.devshawn.kafka.gitops.StateManager;
import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.enums.RackAwarenessPolicy;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import com.devshawn.kafka.gitops.exception.MissingConfigurationException;
import com.devshawn.kafka.gitops.exception.PlanIsUpToDateException;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.exception.WritePlanOutputException;
import com.devshawn.kafka.gitops.service.ParserService;
import com.devshawn.kafka.gitops.util.LogUtil;
import com.devshawn.kafka.gitops.util.PlanReportRenderer;
import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "plan", mixinStandardHelpOptions = true,
        description = "Generate an execution plan of changes to Kafka resources.")
public class PlanCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"-o", "--output"}, paramLabel = "<file>",
            description = "Specify the output file for the plan.")
    private File outputFile;

    @CommandLine.Option(names = {"--include-unchanged"}, description = "Include unchanged resources in the plan file.")
    private boolean includeUnchanged = false;

    @CommandLine.Option(names = "--manage-replication", description = "Deprecated compatibility alias: RF planning is enabled by default.")
    private boolean manageReplication;

    @CommandLine.Option(names = "--no-manage-replication", description = "Exclude replication-factor changes from this plan.")
    private boolean noManageReplication;

    @CommandLine.Option(names = "--allow-replication-decrease", description = "Compatibility option; decreases are always shown. Apply requires its own approval.")
    private boolean allowReplicationDecrease;

    @CommandLine.Option(names = "--replication-rack-policy", description = "RF placement policy: ${COMPLETION-CANDIDATES}. Default: RACK_AWARE.")
    private RackAwarenessPolicy replicationRackPolicy;

    @CommandLine.ParentCommand
    private MainCommand parent;

    @Override
    public Integer call() {
        try {
            if (noManageReplication && (manageReplication || allowReplicationDecrease || replicationRackPolicy != null)) {
                throw new ValidationException("RF_OPTION_CONFLICT: --no-manage-replication cannot be combined with --manage-replication or other RF options.");
            }
            System.out.println("Generating execution plan...\n");
            ParserService parserService = new ParserService(parent.getStateFile());
            System.out.println("[CONFIG] Loading Kafka properties ...");
            StateManager stateManager = parent.createStateManager(generateStateManagerConfig(), parserService);
            var report = stateManager.planDetailed();
            PlanReportRenderer.print(report, parent.isDeleteDisabled(), parent.areAclsDisabled(), parent.areTopicsDisabled());
            if (report.blocked()) return 2;
            stateManager.writeReportedPlan(report);
            return 0;
        } catch (PlanIsUpToDateException ex) {
            LogUtil.printNoChangesMessage();
            return 0;
        } catch (MissingConfigurationException ex) {
            LogUtil.printGenericError(ex);
        } catch (ValidationException ex) {
            LogUtil.printCommandFailure(ex, false);
        } catch (KafkaExecutionException ex) {
            LogUtil.printKafkaExecutionError(ex);
        } catch (WritePlanOutputException ex) {
            LogUtil.printPlanOutputError(ex);
        }
        return 2;
    }

    private ManagerConfig generateStateManagerConfig() {
        return new ManagerConfig.Builder()
                .setVerboseRequested(parent.isVerboseRequested())
                .setDeleteDisabled(parent.isDeleteDisabled())
                .setIncludeUnchangedEnabled(includeUnchanged)
                .setManageReplicationRequested(!noManageReplication)
                .setAllowReplicationDecreaseRequested(allowReplicationDecrease)
                .setReplicationRackPolicy(replicationRackPolicy == null ? RackAwarenessPolicy.RACK_AWARE : replicationRackPolicy)
                .setNullableConfigFile(parent.getConfigFile())
                .setStateFile(parent.getStateFile())
                .setSkipAclsDisabled(parent.areAclsDisabled())
                .setSkipTopicsDisabled(parent.areTopicsDisabled())
                .setNullablePlanFile(outputFile)
                .build();
    }
}
