// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.cli;

import com.devshawn.kafka.gitops.MainCommand;
import com.devshawn.kafka.gitops.StateManager;
import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import com.devshawn.kafka.gitops.exception.MissingConfigurationException;
import com.devshawn.kafka.gitops.exception.PlanIsUpToDateException;
import com.devshawn.kafka.gitops.exception.ReadPlanInputException;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.service.ParserService;
import com.devshawn.kafka.gitops.util.LogUtil;
import com.devshawn.kafka.gitops.util.PlanUtil;
import com.devshawn.kafka.gitops.exception.PlanBlockedException;
import com.devshawn.kafka.gitops.util.PlanReportRenderer;
import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "apply", mixinStandardHelpOptions = true,
        description = "Apply changes to Kafka resources.")
public class ApplyCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"-p", "--plan"}, paramLabel = "<file>",
            description = "Specify the plan file to use.")
    private File planFile;

    @CommandLine.Option(names = "--manage-replication", description = "Deprecated compatibility alias: RF management is enabled by default; fresh preflight is mandatory.")
    private boolean manageReplication;

    @CommandLine.Option(names = "--no-manage-replication", description = "Disable RF management; reject any JSON plan containing an RF operation before all writes.")
    private boolean noManageReplication;

    @CommandLine.Option(names = "--allow-replication-decrease", description = "Allow RF decreases after fresh preflight; --no-delete always forbids a decrease.")
    private boolean allowReplicationDecrease;

    @CommandLine.ParentCommand
    private MainCommand parent;

    @Override
    public Integer call() {
        try {
            if (noManageReplication && (manageReplication || allowReplicationDecrease)) {
                throw new ValidationException("RF_OPTION_CONFLICT: --no-manage-replication cannot be combined with other RF options.");
            }
            System.out.println("Executing apply...\n");
            ParserService parserService = new ParserService(parent.getStateFile());
            System.out.println("[CONFIG] Loading Kafka properties ...");
            StateManager stateManager = parent.createStateManager(generateStateManagerConfig(), parserService);
            DesiredPlan desiredPlan = stateManager.apply();
            LogUtil.printApplyOverview(PlanUtil.getOverview(desiredPlan,
                    parent.isDeleteDisabled(),
                    parent.areAclsDisabled(),
                    parent.areTopicsDisabled()));
            return 0;
        } catch (PlanBlockedException ex) {
            PlanReportRenderer.print(ex.getReport(), parent.isDeleteDisabled(), parent.areAclsDisabled(), parent.areTopicsDisabled());
        } catch (PlanIsUpToDateException ex) {
            LogUtil.printNoChangesMessage();
            return 0;
        } catch (MissingConfigurationException | ReadPlanInputException ex) {
            LogUtil.printGenericError(ex, true);
        } catch (ValidationException ex) {
            LogUtil.printCommandFailure(ex, true);
        } catch (KafkaExecutionException ex) {
            LogUtil.printKafkaExecutionError(ex, true);
        }
        return 2;
    }

    private ManagerConfig generateStateManagerConfig() {
        return new ManagerConfig.Builder()
                .setVerboseRequested(parent.isVerboseRequested())
                .setDeleteDisabled(parent.isDeleteDisabled())
                .setIncludeUnchangedEnabled(false)
                .setManageReplicationRequested(!noManageReplication)
                .setAllowReplicationDecreaseRequested(allowReplicationDecrease)
                .setSkipAclsDisabled(parent.areAclsDisabled())
                .setSkipTopicsDisabled(parent.areTopicsDisabled())
                .setNullableConfigFile(parent.getConfigFile())
                .setStateFile(parent.getStateFile())
                .setNullablePlanFile(planFile)
                .build();
    }
}
