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
import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "apply", description = "Apply changes to Kafka resources.")
public class ApplyCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"-p", "--plan"}, paramLabel = "<file>",
            description = "Specify the plan file to use.")
    private File planFile;

    @CommandLine.Option(names = "--manage-replication", description = "Run RF preflight; RF execution remains unavailable.")
    private boolean manageReplication;

    @CommandLine.Option(names = "--allow-replication-decrease", description = "Permit RF decrease preflight; does not execute RF changes.")
    private boolean allowReplicationDecrease;

    @CommandLine.ParentCommand
    private MainCommand parent;

    @Override
    public Integer call() {
        try {
            if (allowReplicationDecrease && !manageReplication) {
                throw new ValidationException("--allow-replication-decrease requires --manage-replication.");
            }
            System.out.println("Executing apply...\n");
            ParserService parserService = new ParserService(parent.getStateFile());
            StateManager stateManager = parent.createStateManager(generateStateManagerConfig(), parserService);
            DesiredPlan desiredPlan = stateManager.apply();
            LogUtil.printApplyOverview(PlanUtil.getOverview(desiredPlan,
                    parent.isDeleteDisabled(),
                    parent.areAclsDisabled(),
                    parent.areTopicsDisabled()));
            return 0;
        } catch (PlanIsUpToDateException ex) {
            LogUtil.printNoChangesMessage();
            return 0;
        } catch (MissingConfigurationException | ReadPlanInputException ex) {
            LogUtil.printGenericError(ex, true);
        } catch (ValidationException ex) {
            LogUtil.printValidationResult(ex.getMessage(), false);
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
                .setManageReplicationRequested(manageReplication)
                .setAllowReplicationDecreaseRequested(allowReplicationDecrease)
                .setSkipAclsDisabled(parent.areAclsDisabled())
                .setSkipTopicsDisabled(parent.areTopicsDisabled())
                .setNullableConfigFile(parent.getConfigFile())
                .setStateFile(parent.getStateFile())
                .setNullablePlanFile(planFile)
                .build();
    }
}
