// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.exception;

import com.devshawn.kafka.gitops.service.ReassignmentExecutor;

/** Safe diagnostics with the full typed partition result; never retains an external exception message. */
public final class ReplicationExecutionException extends ValidationException {
    public enum Phase { EXECUTION, VERIFICATION, FOLLOWUP, SESSION_CLOSE }
    private final ReassignmentExecutor.ExecutionResult result;
    private final Phase phase;

    public ReplicationExecutionException(ReassignmentExecutor.ExecutionResult result, Phase phase) {
        this(result, phase, null);
    }

    public ReplicationExecutionException(ReassignmentExecutor.ExecutionResult result, Phase phase, KafkaExecutionException stalePartitionPlan) {
        super(message(result, phase) + (stalePartitionPlan == null ? "" : "\n" + stalePartitionPlan.getCode()
                + ": " + stalePartitionPlan.getMessage() + " " + stalePartitionPlan.getExceptionMessage()));
        this.result = java.util.Objects.requireNonNull(result);
        this.phase = java.util.Objects.requireNonNull(phase);
        if (stalePartitionPlan != null) initCause(stalePartitionPlan);
    }

    public ReassignmentExecutor.ExecutionResult getResult() { return result; }
    public Phase getPhase() { return phase; }

    private static String message(ReassignmentExecutor.ExecutionResult result, Phase phase) {
        long completed = result.partitions().values().stream().filter(p -> p.state() == ReassignmentExecutor.State.COMPLETED).count();
        long sent = result.partitions().values().stream().filter(p -> p.submission() == ReassignmentExecutor.Submission.SENT).count();
        String categories = result.partitions().values().stream().map(p -> p.error().name()).distinct().sorted()
                .collect(java.util.stream.Collectors.joining(", "));
        return "RF apply stopped: phase=" + phase + ", state=" + result.state() + ", completed=" + completed
                + "/" + result.requested().size() + ", submission attempts=" + sent + ", reasons=" + categories
                + ". Requests may have been accepted or changes already observed. Inspect Kafka state before retrying; no automatic rollback.";
    }
}
