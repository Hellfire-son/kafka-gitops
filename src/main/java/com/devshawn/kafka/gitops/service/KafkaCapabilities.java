// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureMetadata;

import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.State.*;
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.Reason.*;

/** Feature levels constrain capabilities, not an inferred exact broker release. */
final class KafkaCapabilities {
    private KafkaCapabilities() { }

    static boolean zooKeeper(Config config) {
        String roles = value(config, "process.roles");
        String connection = value(config, "zookeeper.connect");
        return roles != null && roles.isBlank() && connection != null && !connection.isBlank();
    }

    private static boolean metadataBelow(FeatureMetadata features, int firstLevel) {
        if (features == null) return false;
        var active = features.finalizedFeatures().get("metadata.version");
        var supported = features.supportedFeatures().get("metadata.version");
        // A contradictory/future finalized level must never be hidden by a lower advertised bound.
        if (active != null) return active.minVersionLevel() >= 0 && active.maxVersionLevel() < firstLevel;
        return supported != null && supported.minVersion() >= 0 && supported.maxVersion() < firstLevel;
    }

    static CapabilityAssessment cordoning(Config config, FeatureMetadata features) {
        ConfigEntry entry = config == null ? null : config.get("cordoned.log.dirs");
        if (entry != null) {
            if (entry.isSensitive() || entry.value() == null) return new CapabilityAssessment(UNKNOWN, CONFIG_UNAVAILABLE, true);
            return entry.value().isBlank() ? new CapabilityAssessment(SUPPORTED, CONFIG_EXPLICITLY_EMPTY, true)
                    : new CapabilityAssessment(UNSAFE, BROKER_CORDONED, true);
        }
        if (zooKeeper(config)) return new CapabilityAssessment(NOT_APPLICABLE, ZOOKEEPER_MODE_CONFIRMED, true);
        if (metadataBelow(features, 30)) return new CapabilityAssessment(NOT_APPLICABLE, METADATA_LEVEL_BELOW_CORDONING, true);
        return new CapabilityAssessment(UNKNOWN, CAPABILITY_NOT_ESTABLISHED, true);
    }

    static ElrMode elr(FeatureMetadata features, boolean confirmedZooKeeper) {
        if (features != null && features.finalizedFeaturesEpoch().isPresent()
                && features.finalizedFeaturesEpoch().get() < 0) return ElrMode.UNKNOWN;
        if (features != null && features.finalizedFeatures().containsKey("eligible.leader.replicas.version")
                && features.finalizedFeaturesEpoch().isEmpty()) return ElrMode.UNKNOWN;
        if (features != null && features.finalizedFeaturesEpoch().isPresent()) {
            var active = features.finalizedFeatures().get("eligible.leader.replicas.version");
            if (active != null) {
                if (active.minVersionLevel() == 1 && active.maxVersionLevel() == 1) return ElrMode.ENABLED;
                if (active.minVersionLevel() == 0 && active.maxVersionLevel() == 0) return ElrMode.DISABLED;
                return ElrMode.UNKNOWN;
            }
            var support = features.supportedFeatures().get("eligible.leader.replicas.version");
            if (support != null && support.minVersion() == 0 && support.maxVersion() >= 1) return ElrMode.DISABLED;
        }
        // Absence alone is not evidence. These are positive cluster-mode/feature-level proofs.
        if (confirmedZooKeeper || metadataBelow(features, 23)) return ElrMode.NOT_APPLICABLE;
        return ElrMode.UNKNOWN;
    }

    private static String value(Config config, String name) {
        ConfigEntry entry = config == null ? null : config.get(name);
        return entry == null || entry.isSensitive() ? null : entry.value();
    }
}
