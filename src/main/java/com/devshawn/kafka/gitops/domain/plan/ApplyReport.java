// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.config.ManagerConfig;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor.ErrorCategory;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor.ExecutionResult;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor.PartitionOutcome;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor.State;
import com.devshawn.kafka.gitops.service.ReassignmentExecutor.Submission;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.devshawn.kafka.gitops.domain.plan.ApplyReport.Result.*;

/** Per-invocation outcomes for the CLI. This report neither authorizes nor executes changes. */
public final class ApplyReport {
    public enum Result { SUCCESS, NO_OP, FAILED, BLOCKED, NOT_RUN, PARTIAL, UNKNOWN }

    public static final class Step {
        private Result result = NOT_RUN;

        public Result result() { return result; }
        public void record(Result result) { this.result = result; }
    }

    public record ConfigChange(TopicConfigPlan plan, Step step) { }
    public record AclChange(AclPlan plan, Step step) {
        public boolean deleted() {
            return plan.getAction() == PlanAction.REMOVE && (step.result() == SUCCESS || step.result() == PARTIAL);
        }
    }
    public record ReplicationObservation(ExecutionResult execution, boolean verified, boolean verificationAttempted) { }

    public static final class TopicChange {
        private final TopicPlan plan;
        private final Step lifecycle;
        private final Step partitions;
        private final Step replication;
        private final List<ConfigChange> configs;
        private ReplicationObservation replicationObservation;

        private TopicChange(TopicPlan plan) {
            this.plan = plan;
            lifecycle = plan.getAction() == PlanAction.UPDATE ? null : new Step();
            partitions = plan.getPartitionPlan().isPresent() ? new Step() : null;
            replication = plan.getReplicationPlan().isPresent() ? new Step() : null;
            configs = plan.getTopicConfigPlans().stream().filter(c -> c.getAction() != PlanAction.NO_CHANGE)
                    .map(c -> new ConfigChange(c, new Step())).toList();
        }

        public TopicPlan plan() { return plan; }
        public Step lifecycle() { return lifecycle; }
        public Step partitions() { return partitions; }
        public Step replication() { return replication; }
        public List<ConfigChange> configs() { return configs; }
        public Optional<ReplicationObservation> replicationObservation() { return Optional.ofNullable(replicationObservation); }

        public List<Result> stepResults() {
            List<Result> results = new ArrayList<>();
            if (lifecycle != null) results.add(lifecycle.result());
            if (partitions != null) results.add(partitions.result());
            if (replication != null) results.add(replication.result());
            configs.forEach(c -> results.add(c.step().result()));
            return results;
        }
    }

    private final ManagerConfig config;
    private final Map<String, TopicChange> topics = new LinkedHashMap<>();
    private final Set<String> planningBlockers = new TreeSet<>();
    private List<AclChange> acls = List.of();
    private PlanningReport planning;
    private long unchangedEntries;
    private boolean preflightBlocked;

    public ApplyReport(ManagerConfig config) {
        this.config = config;
    }

    public void prepare(DesiredPlan plan, PlanningReport planning) {
        this.planning = planning;
        if (!skipTopics()) {
            if (planning != null) planning.issues().stream().filter(i -> i.severity() == PlanningReport.Severity.BLOCKED)
                    .forEach(i -> i.topic().ifPresent(planningBlockers::add));
            unchangedEntries = plan.getTopicPlans().stream().filter(t -> t.getAction() == PlanAction.NO_CHANGE
                    && !planningBlockers.contains(t.getName())).map(TopicPlan::getName).distinct().count();
            plan.getTopicPlans().stream().filter(t -> enabled(t.getAction()))
                    .forEach(t -> topics.put(t.getName(), new TopicChange(t)));
        }
        if (!skipAcls()) acls = plan.getAclPlans().stream().filter(a -> enabled(a.getAction()))
                .map(a -> new AclChange(a, new Step())).toList();
    }

    private boolean enabled(PlanAction action) {
        return action != PlanAction.NO_CHANGE && (action != PlanAction.REMOVE || !config.isDeleteDisabled());
    }

    public boolean skipTopics() { return config.isSkipTopicsDisabled(); }
    public boolean skipAcls() { return config.isSkipAclsDisabled(); }
    public Collection<TopicChange> topics() { return topics.values(); }
    public TopicChange topic(String name) { return topics.get(name); }
    public List<AclChange> acls() { return acls; }
    public Optional<PlanningReport> planning() { return Optional.ofNullable(planning); }
    public long unchangedEntries() { return unchangedEntries; }
    public Set<String> planningBlockers() { return planningBlockers; }
    public boolean isBlocked() { return preflightBlocked || (planning != null && planning.blocked()); }

    public void blockPartitionPreflight(String topic) {
        preflightBlocked = true;
        var change = topics.get(topic);
        if (change != null && change.partitions() != null) change.partitions().record(BLOCKED);
    }

    public void blockReplicationPreflight(ReplicationPreflightException error) {
        preflightBlocked = true;
        error.getResults().forEach((name, result) -> {
            var change = topics.get(name);
            if (change != null && change.replication() != null && result.reasons().stream()
                    .anyMatch(finding -> finding.code() != ReplicationSafetyResult.Reason.PREFLIGHT_NOT_PERFORMED)) {
                change.replication().record(BLOCKED);
            }
        });
    }

    public void recordReplication(ExecutionResult execution, boolean verified, boolean verificationAttempted) {
        Map<String, Map<TopicPartition, PartitionOutcome>> byTopic = new LinkedHashMap<>();
        execution.partitions().forEach((partition, outcome) ->
                byTopic.computeIfAbsent(partition.topic(), name -> new LinkedHashMap<>()).put(partition, outcome));
        topics.values().stream().filter(t -> t.replication() != null).forEach(change -> {
            var results = byTopic.get(change.plan().getName());
            if (results == null) {
                change.replication().record(UNKNOWN);
            } else {
                var observation = new ReplicationObservation(new ExecutionResult(results.keySet(), results), verified, verificationAttempted);
                change.replicationObservation = observation;
                change.replication().record(replicationResult(change.plan().getReplicationPlan().orElseThrow(), observation));
            }
        });
    }

    private static Result replicationResult(TopicReplicationPlan plan, ReplicationObservation observed) {
        var outcomes = observed.execution().partitions();
        Set<Integer> differing = plan.getAssignments().stream().filter(a -> !a.getTargetReplicas().equals(a.getPreviousReplicas()))
                .map(PartitionReplicaAssignment::getPartition).collect(Collectors.toSet());
        boolean changed = outcomes.entrySet().stream().anyMatch(entry -> entry.getValue().state() == State.COMPLETED
                && entry.getValue().submission() == Submission.SENT && differing.contains(entry.getKey().partition()));
        if (observed.verified()) {
            if (changed) return SUCCESS;
            return outcomes.values().stream().anyMatch(outcome -> outcome.submission() == Submission.UNKNOWN) ? UNKNOWN : NO_OP;
        }
        if (changed) return PARTIAL;
        if (observed.verificationAttempted() || outcomes.values().stream().anyMatch(outcome -> outcome.state() != State.FAILED
                && outcome.error() != ErrorCategory.NOT_SUBMITTED)) return UNKNOWN;
        if (outcomes.values().stream().anyMatch(outcome -> switch (outcome.error()) {
            case PREFLIGHT_BLOCKED, CONFLICT, STALE_PLAN, ELR_UNSAFE, ELR_UNKNOWN -> true;
            default -> false;
        })) return BLOCKED;
        if (outcomes.values().stream().anyMatch(outcome -> outcome.state() == State.FAILED
                && outcome.error() != ErrorCategory.NOT_SUBMITTED)) return FAILED;
        return NOT_RUN;
    }

    public static Result combine(List<Result> steps) {
        if (steps.contains(PARTIAL)) return PARTIAL;
        if (steps.contains(SUCCESS)) return steps.stream().allMatch(r -> r == SUCCESS || r == NO_OP) ? SUCCESS : PARTIAL;
        if (steps.contains(UNKNOWN)) return UNKNOWN;
        if (steps.contains(FAILED)) return FAILED;
        if (steps.contains(BLOCKED)) return BLOCKED;
        return !steps.isEmpty() && steps.stream().allMatch(r -> r == NO_OP) ? NO_OP : NOT_RUN;
    }
}
