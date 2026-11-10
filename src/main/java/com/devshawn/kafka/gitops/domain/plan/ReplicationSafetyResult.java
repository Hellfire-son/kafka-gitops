package com.devshawn.kafka.gitops.domain.plan;

import java.util.List;
import java.util.OptionalInt;

/** Typed decisions. Diagnostic text is derived from enums, never from Kafka response strings. */
public record ReplicationSafetyResult(List<Finding> reasons, List<Warning> warnings,
                                       List<PartitionDecision> operations) {
    public ReplicationSafetyResult {
        reasons = List.copyOf(reasons);
        warnings = List.copyOf(warnings);
        operations = List.copyOf(operations);
    }

    public Status status() { return reasons.isEmpty() ? Status.READY : Status.BLOCKED; }

    public enum Status { READY, BLOCKED }
    public enum Condition { INVALID, UNKNOWN }
    public enum Operation { INCREASE, DECREASE, UNCHANGED }
    public enum Warning { NON_ATOMIC_SNAPSHOT, BROKER_ONLY, LEADER_NOT_PINNED_IN_SCHEMA_V1, EXECUTION_UNAVAILABLE }
    public enum Reason {
        STRUCTURE_INVALID, INVALID_PERMISSION_COMBINATION, MANAGE_REPLICATION_REQUIRED, DECREASE_PERMISSION_REQUIRED,
        NO_DELETE, CLUSTER_ID_CHANGED, TOPIC_NOT_FOUND, TOPIC_ID_CHANGED, PARTITION_COUNT_CHANGED,
        PREVIOUS_REPLICAS_CHANGED, SNAPSHOT_NOT_FRESH, REASSIGNMENT_UNKNOWN, ACTIVE_REASSIGNMENT,
        LEADER_UNKNOWN, LEADER_NOT_IN_ISR, LEADER_NOT_RETAINED, ISR_INCOMPLETE, TARGET_NOT_IN_ISR, MIN_ISR_UNKNOWN,
        MIN_ISR_CHANGED, TARGET_BELOW_MIN_ISR, CONFIG_CONFLICT, PARTITION_CONFLICT,
        TOPOLOGY_CHANGED, RACK_METADATA_UNKNOWN, RACK_POLICY_VIOLATION,
        BROKER_OFFLINE, BROKER_HEALTH_UNKNOWN, BROKER_FENCED, BROKER_FENCING_UNKNOWN,
        STORAGE_UNAVAILABLE, STORAGE_UNKNOWN, BROKER_CORDONED, CORDONING_CHANGED, CORDONING_UNKNOWN,
        PLACEMENT_FORBIDDEN, PLACEMENT_UNKNOWN, DISK_INSUFFICIENT, DISK_CAPACITY_UNKNOWN,
        ELR_MODE_UNKNOWN, ELR_UNSUPPORTED, FEATURES_CHANGED, CONFIG_STATE_UNKNOWN, CONFIG_CHANGED,
        METADATA_UNAVAILABLE, PREFLIGHT_NOT_PERFORMED
    }

    public record Finding(Reason code, Condition condition, OptionalInt partition, OptionalInt brokerId) {
        public static Finding global(Reason code, Condition condition) {
            return new Finding(code, condition, OptionalInt.empty(), OptionalInt.empty());
        }
    }

    public record PartitionDecision(int partition, Operation operation, List<Finding> reasons) {
        public PartitionDecision { reasons = List.copyOf(reasons); }
        public Status status() { return reasons.isEmpty() ? Status.READY : Status.BLOCKED; }
    }
}
