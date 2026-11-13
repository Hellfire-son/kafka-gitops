// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.util.ReplicationPlanValidation;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Internal execution contract. Production CLI remains blocked until all live safety evidence can be verified.
 * Implementations must recheck deadline, permissions and live preconditions; READY is not a durable capability.
 * ACCEPTED acknowledges an RPC only. COMPLETED requires observed assignments, ISR and no active reassignment.
 * Timeout/interruption never implies cancellation or rollback of already accepted Kafka work.
 */
public interface ReassignmentExecutor {
    ExecutionResult execute(Request request);

    enum State { ACCEPTED, RUNNING, COMPLETED, FAILED, UNKNOWN }
    enum ErrorCategory { NONE, VALIDATION, AUTHORIZATION, UNSUPPORTED, STALE_PLAN, BROKER_UNAVAILABLE,
        TIMEOUT, INTERRUPTED, PARTIAL_FAILURE, INTERNAL, UNKNOWN, PREFLIGHT_BLOCKED, CONFLICT, NOT_SUBMITTED,
        RPC_TIMEOUT, DEADLINE_EXCEEDED, ELR_UNKNOWN, ELR_UNSAFE }

    /** SENT means an attempted submission, not acceptance. UNKNOWN means the integration lost that evidence. */
    enum Submission { NOT_SENT, SENT, OBSERVED, UNKNOWN }

    record PreparedOperation(String topicName, TopicReplicationPlan plan, ReplicationSafetyResult preflight) {
        public PreparedOperation {
            ReplicationPlanValidation.validate(plan);
            if (preflight == null || preflight.status() != ReplicationSafetyResult.Status.READY
                    || topicName == null || !topicName.matches("[a-zA-Z0-9._-]{1,249}") || topicName.equals(".") || topicName.equals("..")
                    || preflight.operations().size() != plan.getPartitionCount()
                    || !preflight.operations().stream().map(o -> o.partition()).collect(java.util.stream.Collectors.toSet())
                    .equals(plan.getAssignments().stream().map(a -> a.getPartition()).collect(java.util.stream.Collectors.toSet()))
                    || preflight.operations().stream().anyMatch(o -> o.status() != ReplicationSafetyResult.Status.READY)) {
                throw new IllegalArgumentException("Executor requires a named RF operation and a READY preflight.");
            }
        }
    }

    record Request(List<PreparedOperation> operations, ReplicationSafetyPolicy.Permissions permissions,
                   long startedNanos, long deadlineNanos) {
        public Request {
            operations = List.copyOf(operations);
            if (operations.isEmpty() || deadlineNanos - startedNanos <= 0 || permissions == null
                    || !permissions.manageReplication()
                    || operations.stream().map(PreparedOperation::topicName).distinct().count() != operations.size()
                    || operations.stream().anyMatch(o -> o.plan().requiresDecreaseApproval()
                    && (!permissions.allowDecrease() || permissions.noDelete()))) {
                throw new IllegalArgumentException("Executor requires permitted operations and a positive shared deadline.");
            }
        }
    }

    record PartitionOutcome(State state, ErrorCategory error, Submission submission, State lastKnownState) {
        public PartitionOutcome(State state, ErrorCategory error) {
            this(state, error, Submission.NOT_SENT, state);
        }

        public PartitionOutcome {
            if (state == null || error == null || submission == null || lastKnownState == null
                    || ((state == State.COMPLETED || state == State.ACCEPTED || state == State.RUNNING)
                    && error != ErrorCategory.NONE)
                    || ((state == State.FAILED || state == State.UNKNOWN) && error == ErrorCategory.NONE)) {
                throw new IllegalArgumentException("Invalid reassignment outcome classification.");
            }
        }
    }

    record ExecutionResult(Set<TopicPartition> requested, Map<TopicPartition, PartitionOutcome> partitions) {
        public ExecutionResult {
            requested = Set.copyOf(requested);
            partitions = Map.copyOf(partitions);
            if (requested.isEmpty() || !partitions.keySet().equals(requested)) {
                throw new IllegalArgumentException("Every requested partition needs an explicit outcome, including UNKNOWN.");
            }
        }

        public State state() {
            if (partitions.values().stream().allMatch(p -> p.state() == State.COMPLETED)) return State.COMPLETED;
            if (partitions.values().stream().anyMatch(p -> p.state() == State.FAILED)) return State.FAILED;
            if (partitions.values().stream().anyMatch(p -> p.state() == State.UNKNOWN)) return State.UNKNOWN;
            if (partitions.values().stream().anyMatch(p -> p.state() == State.RUNNING)) return State.RUNNING;
            return State.ACCEPTED;
        }

        public boolean partiallyCompleted() {
            return state() != State.COMPLETED && partitions.values().stream().anyMatch(p -> p.state() == State.COMPLETED);
        }
    }
}
