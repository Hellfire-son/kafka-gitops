// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Set;

public final class TopicConfigSecurity {
    private static final Set<String> SAFE_CONFIG_KEYS = Set.of(
            TopicConfig.SEGMENT_BYTES_CONFIG, TopicConfig.SEGMENT_MS_CONFIG,
            TopicConfig.SEGMENT_JITTER_MS_CONFIG, TopicConfig.SEGMENT_INDEX_BYTES_CONFIG,
            TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, TopicConfig.FLUSH_MS_CONFIG,
            TopicConfig.RETENTION_BYTES_CONFIG, TopicConfig.RETENTION_MS_CONFIG,
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG,
            TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
            TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, TopicConfig.FILE_DELETE_DELAY_MS_CONFIG,
            TopicConfig.DELETE_RETENTION_MS_CONFIG, TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, TopicConfig.COMPRESSION_TYPE_CONFIG,
            // Removed from the Kafka 4.x Java API; retain legacy plan/config diagnostics.
            TopicConfig.PREALLOCATE_CONFIG, "message.format.version",
            TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "message.timestamp.difference.max.ms",
            TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG,
            TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG,
            "leader.replication.throttled.replicas", "follower.replication.throttled.replicas",
            // Keep this spelling in legacy diagnostic plans readable without rewriting config keys.
            "message.max.bytes"
    );

    private TopicConfigSecurity() {
    }

    // Unknown names are confidential by default, including in legacy plans and for new topics.
    // Broker sensitivity metadata always takes precedence over this diagnostic allowlist.
    public static boolean isSensitive(String key) {
        return !SAFE_CONFIG_KEYS.contains(key);
    }

    public static boolean isSensitive(TopicConfigPlan config) {
        return config.isSensitive() || isSensitive(config.getKey());
    }

    public static boolean hasSensitiveValues(TopicPlan topic) {
        boolean configValues = topic.getTopicConfigPlans().stream().anyMatch(config ->
                isSensitive(config) && (config.getValue().isPresent() || config.getPreviousValue().isPresent()));
        boolean detailValues = topic.getTopicDetails().map(details -> details.getConfigs().keySet().stream()
                .anyMatch(key -> isSensitive(key) || topic.getTopicConfigPlans().stream()
                        .anyMatch(config -> config.getKey().equals(key) && isSensitive(config))))
                .orElse(false);
        return configValues || detailValues;
    }
}
