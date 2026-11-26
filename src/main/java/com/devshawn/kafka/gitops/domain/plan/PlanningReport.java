// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.*;

/** Text diagnostics are deliberately outside the executable JSON schema. */
public record PlanningReport(DesiredPlan changes, Map<String, TopicPreview> topics, List<Issue> issues) {
    public PlanningReport { topics = Collections.unmodifiableMap(new TreeMap<>(topics)); issues = List.copyOf(issues); }
    public boolean blocked() { return issues.stream().anyMatch(i -> i.severity() == Severity.BLOCKED); }
    public enum Severity { WARNING, BLOCKED }
    public record Issue(Severity severity, String code, String resource, String actual, String required,
                        String state, String reason, String action, Optional<String> topic) { }
    public record TopicPreview(TopicDetails desired, boolean existenceKnown, boolean exists,
                               Optional<TopicDescription> current, Map<Integer, ReplicationClusterSnapshot.Broker> brokers,
                               Map<String, String> currentConfigValues, Set<String> sensitiveConfigKeys,
                               Optional<ReplicationClusterSnapshot> replication) {
        public TopicPreview(TopicDetails desired, boolean existenceKnown, boolean exists, Optional<TopicDescription> current,
                            Map<Integer, ReplicationClusterSnapshot.Broker> brokers, Map<String, String> currentConfigValues) {
            this(desired, existenceKnown, exists, current, brokers, currentConfigValues, Set.of(), Optional.empty());
        }
        public TopicPreview {
            brokers = Map.copyOf(brokers);
            currentConfigValues = Map.copyOf(currentConfigValues);
            sensitiveConfigKeys = Set.copyOf(sensitiveConfigKeys);
        }
    }
}
