package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.plan.*;
import com.devshawn.kafka.gitops.exception.ReplicationExecutionException;
import com.devshawn.kafka.gitops.exception.ReplicationPreflightException;
import com.devshawn.kafka.gitops.util.PlanValidation;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.devshawn.kafka.gitops.service.ReassignmentExecutor.*;

/** Owns exactly one RF Admin session for preflight, execution and independent completion confirmation. */
final class AdminReplicationApplication implements ReplicationApplication {
    private final Supplier<Admin> admins;
    private final AdminReassignmentExecutor.Settings settings;
    private final AdminReassignmentExecutor.TimeSource time;
    private final AdminReassignmentExecutor.SnapshotReader snapshots;
    private final Duration operationTimeout;
    private final BiFunction<AdminReassignmentExecutor, Request, ExecutionResult> execution;

    AdminReplicationApplication(Supplier<Admin> admins) {
        this(admins, new AdminReassignmentExecutor.Settings(100, Duration.ofSeconds(10), Duration.ofMillis(100)),
                new AdminReassignmentExecutor.TimeSource() {
                    public long nanoTime() { return System.nanoTime(); }
                    public void pause(long nanos) throws InterruptedException { TimeUnit.NANOSECONDS.sleep(nanos); }
                }, (admin, topics, timeout) -> ReplicationMetadataReader.read(admin, topics, timeout, true),
                Duration.ofMinutes(10), AdminReassignmentExecutor::execute);
    }

    /** Package-local trusted test dependencies only. No connection or safety configuration is loaded from plans. */
    AdminReplicationApplication(Supplier<Admin> admins, AdminReassignmentExecutor.Settings settings,
                                AdminReassignmentExecutor.TimeSource time, AdminReassignmentExecutor.SnapshotReader snapshots,
                                Duration operationTimeout, BiFunction<AdminReassignmentExecutor, Request, ExecutionResult> execution) {
        this.admins = Objects.requireNonNull(admins);
        this.settings = Objects.requireNonNull(settings);
        this.time = Objects.requireNonNull(time);
        this.snapshots = Objects.requireNonNull(snapshots);
        this.execution = Objects.requireNonNull(execution);
        if (operationTimeout == null || operationTimeout.isZero() || operationTimeout.isNegative()
                || operationTimeout.compareTo(Duration.ofHours(24)) > 0) throw new IllegalArgumentException("Invalid RF operation deadline.");
        this.operationTimeout = operationTimeout;
    }

    @Override public ExecutionResult apply(List<TopicPlan> topics, ReplicationSafetyPolicy.Permissions permissions) {
        DesiredPlan plan = new DesiredPlan.Builder().addAllTopicPlans(topics).build();
        PlanValidation.validate(plan);
        if (topics.isEmpty() || topics.stream().anyMatch(t -> t.getReplicationPlan().isEmpty())) throw new IllegalArgumentException("RF integration requires explicit RF operations.");
        ReplicationSafetyPolicy policy = new ReplicationSafetyPolicy();
        Map<String, ReplicationSafetyResult> checks = new TreeMap<>();
        topics.forEach(t -> checks.put(t.getName(), policy.blockedBeforeMetadata(t.getReplicationPlan().orElseThrow(),
                policy.permissions(t.getReplicationPlan().orElseThrow(), permissions))));
        if (topics.stream().anyMatch(t -> !policy.permissions(t.getReplicationPlan().orElseThrow(), permissions).isEmpty())) {
            throw new ReplicationPreflightException(checks);
        }
        long started = time.nanoTime();
        long deadline = started + operationTimeout.toNanos();
        Admin admin = null;
        ExecutionResult result = null;
        boolean executionStarted = false;
        boolean verificationStarted = false;
        RuntimeException failure = null;
        Request request = null;
        try {
            admin = Objects.requireNonNull(admins.get());
            long readStarted = time.nanoTime();
            ReplicationClusterSnapshot snapshot = snapshots.read(admin, checks.keySet(), timeoutMs(deadline));
            long now = time.nanoTime();
            for (TopicPlan topic : topics) {
                var rf = topic.getReplicationPlan().orElseThrow();
                var context = ReplicationSafetyPolicy.Constraints.forTopic(topic, readStarted, now);
                checks.put(topic.getName(), policy.evaluateForExecution(topic.getName(), rf, snapshot, permissions, context,
                        completed(topic.getName(), rf, snapshot)));
            }
            if (checks.values().stream().anyMatch(c -> c.status() != ReplicationSafetyResult.Status.READY)) throw new ReplicationPreflightException(checks);
            checks.forEach((name, check) -> check.warnings().forEach(warning ->
                    System.out.println("[WARNING] " + warning + " Topic: " + name + "; fresh preflight passed; this operational risk remains.")));
            request = new Request(topics.stream().map(t -> new PreparedOperation(t.getName(), t.getReplicationPlan().orElseThrow(), checks.get(t.getName()))).toList(), permissions, started, deadline);
            AdminReassignmentExecutor executor = new AdminReassignmentExecutor(admin, settings, time, snapshots);
            executionStarted = true;
            result = execution.apply(executor, request);
            if (result == null || !result.requested().equals(partitions(topics))) {
                result = unknown(topics, ErrorCategory.INTERNAL);
            }
            if (result.state() != State.COMPLETED) throw new ReplicationExecutionException(result, ReplicationExecutionException.Phase.EXECUTION);
            verificationStarted = true;
            if (!executor.verifyCompleted(request)) {
                result = unknown(topics, ErrorCategory.STALE_PLAN);
                throw new ReplicationExecutionException(result, ReplicationExecutionException.Phase.VERIFICATION);
            }
            return result;
        } catch (ReplicationPreflightException | ReplicationExecutionException ex) {
            failure = ex;
            throw ex;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            if (executionStarted) {
                result = result == null || result.state() == State.COMPLETED ? unknown(topics,
                        AdminReassignmentExecutor.category(ex)) : result;
                failure = new ReplicationExecutionException(result, verificationStarted
                        ? ReplicationExecutionException.Phase.VERIFICATION : ReplicationExecutionException.Phase.EXECUTION);
            } else {
                topics.forEach(t -> checks.put(t.getName(), policy.blockedBeforeMetadata(t.getReplicationPlan().orElseThrow(),
                        List.of(ReplicationSafetyResult.Finding.global(ReplicationSafetyResult.Reason.METADATA_UNAVAILABLE, ReplicationSafetyResult.Condition.UNKNOWN)))));
                failure = new ReplicationPreflightException(checks);
            }
            throw failure;
        } finally {
            if (admin != null) {
                try { admin.close(Duration.ZERO); }
                catch (RuntimeException ignored) {
                    if (failure == null && result != null) throw new ReplicationExecutionException(result, ReplicationExecutionException.Phase.SESSION_CLOSE);
                }
            }
        }
    }

    private static Set<Integer> completed(String name, TopicReplicationPlan plan, ReplicationClusterSnapshot snapshot) {
        Set<Integer> complete = new HashSet<>();
        if (snapshot == null || !snapshot.topics().containsKey(name)) return complete;
        for (var assignment : plan.getAssignments()) {
            var part = snapshot.topics().get(name).partitions().stream().filter(p -> p.id() == assignment.getPartition()).findFirst().orElse(null);
            if (part != null && !snapshot.reassignments().contains(new TopicPartition(name, part.id()))
                    && part.replicas().equals(assignment.getTargetReplicas()) && part.isr().containsAll(part.replicas())
                    && part.leader().isPresent() && part.replicas().contains(part.leader().getAsInt()) && part.isr().contains(part.leader().getAsInt())) complete.add(part.id());
        }
        return complete;
    }

    private int timeoutMs(long deadline) {
        long left = deadline - time.nanoTime();
        if (left <= 0 || Thread.currentThread().isInterrupted()) throw new IllegalStateException("RF deadline unavailable.");
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(Math.min(left, settings.rpcTimeout().toNanos())));
    }
    private static Set<TopicPartition> partitions(List<TopicPlan> topics) {
        Set<TopicPartition> keys = new HashSet<>();
        topics.forEach(t -> t.getReplicationPlan().orElseThrow().getAssignments().forEach(a -> keys.add(new TopicPartition(t.getName(), a.getPartition()))));
        return keys;
    }
    private static ExecutionResult unknown(List<TopicPlan> topics, ErrorCategory error) {
        Map<TopicPartition, PartitionOutcome> outcomes = new HashMap<>();
        partitions(topics).forEach(p -> outcomes.put(p, new PartitionOutcome(State.UNKNOWN, error, Submission.UNKNOWN, State.UNKNOWN)));
        return new ExecutionResult(outcomes.keySet(), outcomes);
    }
}
