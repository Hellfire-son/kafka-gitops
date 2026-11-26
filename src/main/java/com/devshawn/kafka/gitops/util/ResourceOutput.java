// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.AclPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicConfigPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicPartitionPlan;
import com.devshawn.kafka.gitops.domain.state.TopicDetails;
import com.devshawn.kafka.gitops.enums.PlanAction;

import java.util.SortedSet;
import java.util.TreeMap;

/** Common fields for the human-readable plan and apply reports. */
final class ResourceOutput {
    private ResourceOutput() { }

    static void heading(String command, PlanAction action, String resource, String name) {
        String operation = switch (action) {
            case ADD -> "CREATE";
            case UPDATE -> "UPDATE";
            case REMOVE -> "DELETE";
            case NO_CHANGE -> throw new IllegalArgumentException("No change to display");
        };
        System.out.println("[" + command + "] [" + operation + "] " + resource + ": " + name);
    }

    static void topicCreation(TopicDetails details) {
        System.out.println("  Partitions: " + details.getPartitions());
        System.out.println("  Replication factor: " + details.getReplication().map(Object::toString).orElse("UNKNOWN"));
        new TreeMap<>(details.getConfigs()).forEach((key, value) ->
                System.out.println("  Config " + key + ": " + (TopicConfigSecurity.isSensitive(key) ? "[redacted]" : value)));
    }

    static String partitions(TopicPartitionPlan partition) {
        return "  Partitions: " + partition.getPreviousCount() + " -> " + partition.getTargetCount();
    }

    static String replication(SortedSet<Integer> actual, String missing, String target) {
        String before = actual.isEmpty() ? missing : actual.size() == 1 ? actual.first().toString() : actual.toString();
        return "  Replication factor: " + before + " -> " + target;
    }

    static String config(TopicConfigPlan config, String observedValue) {
        boolean sensitive = TopicConfigSecurity.isSensitive(config);
        String before = sensitive ? "[redacted]" : config.getPreviousValue().orElse(observedValue);
        String after = config.getAction() == PlanAction.REMOVE ? "DEFAULT (remove override)"
                : sensitive ? "[redacted]" : config.getValue().orElse("UNKNOWN");
        return "  Config " + config.getKey() + ": " + before + " -> " + after + " [" + config.getAction() + "]";
    }

    static void acl(String command, AclPlan plan) {
        var acl = plan.getAclDetails();
        heading(command, plan.getAction(), "ACL", plan.getName()
                + " (resource=" + acl.getName() + ", operation=" + acl.getOperation() + ")");
        System.out.println("  Resource type: " + acl.getType() + ", pattern: " + acl.getPattern());
        System.out.println("  Principal: " + acl.getPrincipal() + ", host: " + acl.getHost() + ", permission: " + acl.getPermission());
    }

    static void topicDeletion() {
        System.out.println("  Destructive operation: deletes topic data.");
    }
}
