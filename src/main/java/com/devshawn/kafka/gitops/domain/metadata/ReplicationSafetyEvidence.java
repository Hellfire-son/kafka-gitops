package com.devshawn.kafka.gitops.domain.metadata;

import java.util.Map;
import java.util.OptionalLong;

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation;

/** In-memory observations only; never accepted from a saved plan as authorization. */
public record ReplicationSafetyEvidence(OptionalLong completedAtNanos, ElrMode elrMode,
                                        Observation featuresStable, Map<String, Observation> configsStable,
                                        Map<Integer, Observation> cordoningStable,
                                        Map<Integer, Admission> admissions) {
    public ReplicationSafetyEvidence {
        configsStable = Map.copyOf(configsStable);
        cordoningStable = Map.copyOf(cordoningStable);
        admissions = Map.copyOf(admissions);
    }

    public enum ElrMode { DISABLED, ENABLED, UNKNOWN, NOT_APPLICABLE }

    public record Admission(Observation placementAllowed, Observation diskCapacitySufficient) {
        public static Admission unknown() {
            return new Admission(Observation.UNKNOWN, Observation.UNKNOWN);
        }
    }

    public static ReplicationSafetyEvidence unknown() {
        return new ReplicationSafetyEvidence(OptionalLong.empty(), ElrMode.UNKNOWN, Observation.UNKNOWN,
                Map.of(), Map.of(), Map.of());
    }
}
