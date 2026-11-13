// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.domain.metadata;

/** Ephemeral capability evidence. Reasons are codes, never Kafka config values or exception messages. */
public record CapabilityAssessment(State state, Reason reason, boolean critical) {
    public CapabilityAssessment { java.util.Objects.requireNonNull(state); java.util.Objects.requireNonNull(reason); }
    public enum State { SUPPORTED, NOT_APPLICABLE, UNKNOWN, UNSAFE }
    public enum Capability { FENCING, CORDONING, PLACEMENT_ADMISSION, RACK, STORAGE, DISK_CAPACITY }
    public enum Reason {
        LIVE_BROKER_METADATA, BROKER_FENCED, CONFIG_EXPLICITLY_EMPTY, BROKER_CORDONED,
        METADATA_LEVEL_BELOW_CORDONING, ZOOKEEPER_MODE_CONFIRMED, CAPABILITY_NOT_ESTABLISHED,
        CONFIG_UNAVAILABLE, LOG_DIRECTORIES_HEALTHY, LOG_DIRECTORY_ERROR, LOG_DIRECTORIES_UNAVAILABLE,
        RACK_REPORTED, RACK_NOT_REPORTED, POSITIVE_USABLE_BYTES, USABLE_BYTES_NOT_REPORTED, NO_USABLE_BYTES,
        NO_READ_ONLY_PLACEMENT_ADMISSION_API, ELR_NOT_APPLICABLE, ELR_DISABLED, ELR_VERIFIED_EMPTY,
        ELR_MODE_UNESTABLISHED, ELR_METADATA_UNAVAILABLE, ELR_METADATA_INVALID, ELR_NONEMPTY,
        LAST_KNOWN_ELR_NONEMPTY, ELR_ISR_UNHEALTHY
    }
    public boolean blocks() { return state == State.UNSAFE || critical && state == State.UNKNOWN; }
}
