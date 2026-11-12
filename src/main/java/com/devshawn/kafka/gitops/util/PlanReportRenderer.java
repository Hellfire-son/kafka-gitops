package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.*;
import com.devshawn.kafka.gitops.enums.PlanAction;
import java.util.*;

/** Render known changes first, then risks/blockers, finally the command outcome. */
public final class PlanReportRenderer {
    private PlanReportRenderer() { }
    public static void print(PlanningReport report, boolean noDelete, boolean skipAcls, boolean skipTopics) {
        report.topics().forEach((name, preview) -> {
            var current = preview.current();
            String missing = preview.existenceKnown() && !preview.exists() ? "ABSENT" : "UNKNOWN";
            System.out.println("[PLAN] Topic: " + name);
            System.out.println("  Partitions: " + current.map(t -> Integer.toString(t.partitions().size())).orElse(missing)
                    + " -> " + preview.desired().getPartitions());
            System.out.println("  Replication: " + current.map(t -> new TreeSet<>(t.partitions().stream().map(p -> p.replicas().size()).toList()).toString()).orElse(missing)
                    + " -> " + preview.desired().getReplication().map(Object::toString).orElse("UNKNOWN"));
            current.ifPresent(t -> {
                System.out.println("  Current ordered assignments:");
                t.partitions().stream().sorted(Comparator.comparingInt(p -> p.partition())).forEach(p ->
                        System.out.println("    Partition " + p.partition() + ": " + p.replicas().stream().map(n -> n.id()).toList()));
            });
            TopicPlan topic = report.changes().getTopicPlans().stream().filter(t -> t.getName().equals(name)).findFirst().orElse(null);
            if (topic != null && topic.getReplicationPlan().isPresent()) {
                System.out.println("  Proposed ordered assignments (subject to fresh apply preflight):");
                topic.getReplicationPlan().get().getAssignments().forEach(a ->
                        System.out.println("    Partition " + a.getPartition() + ": " + a.getTargetReplicas()));
            } else if (current.isPresent() && preview.desired().getReplication().isPresent()
                    && current.get().partitions().stream().anyMatch(p -> p.replicas().size() != preview.desired().getReplication().get())) {
                System.out.println("  Proposed assignments: NOT_CALCULATED (see diagnostics)");
            }
            if (topic != null) topic.getTopicConfigPlans().stream().filter(c -> c.getAction() != PlanAction.NO_CHANGE).forEach(c -> {
                boolean secret = TopicConfigSecurity.isSensitive(c);
                String before = secret ? "[redacted]" : c.getPreviousValue().orElse(preview.currentConfigValues().getOrDefault(c.getKey(), "UNKNOWN"));
                String after = secret ? "[redacted]" : c.getValue().orElse("DEFAULT (effective value not asserted)");
                System.out.println("  Config " + c.getKey() + ": " + before + " -> " + after + " [" + c.getAction() + "]");
            });
            if (topic != null && topic.getAction() == PlanAction.ADD) preview.desired().getConfigs().forEach((key, value) ->
                    System.out.println("  Config " + key + ": ABSENT -> " + (TopicConfigSecurity.isSensitive(key) ? "[redacted]" : value)));
            if (topic == null) new TreeMap<>(preview.desired().getConfigs()).forEach((key, value) -> {
                boolean secret = TopicConfigSecurity.isSensitive(key);
                System.out.println("  Config " + key + ": " + (secret ? "[redacted]" : preview.currentConfigValues().getOrDefault(key, missing))
                        + " -> " + (secret ? "[redacted]" : value) + " [PREVIEW_ONLY; operation not calculated]");
            });
            preview.brokers().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> {
                var broker = e.getValue();
                System.out.println("  Broker " + e.getKey() + ": " + (broker.isPreviewCandidate() && broker.fencingEstablished() ? "ELIGIBLE_FOR_PREVIEW" : "EXCLUDED")
                        + ", rack=" + broker.rack().orElse("UNKNOWN") + ", storage=" + broker.storage()
                        + ", fencing=" + broker.unfenced() + ", cordoning=" + broker.uncordoned()
                        + ", usableBytes=" + (broker.minimumUsableBytes().isPresent() ? broker.minimumUsableBytes().getAsLong() : "UNKNOWN"));
                broker.capabilities().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(c ->
                        System.out.println("    " + c.getKey() + ": " + c.getValue().state() + " (" + c.getValue().reason() + ")"));
            });
        });
        report.changes().getTopicPlans().stream().filter(t -> t.getAction() == PlanAction.REMOVE).forEach(t ->
                System.out.println("[PLAN] Topic: " + t.getName() + "; actual: PRESENT; desired: ABSENT; change: REMOVE"));
        // ACL actions use the existing renderer; their calculation/semantics are unchanged.
        report.changes().getAclPlans().stream().filter(a -> a.getAction() != PlanAction.NO_CHANGE).forEach(a -> {
            System.out.println("[PLAN] ACL: " + a.getName() + "; resource=" + a.getAclDetails().getName()
                    + "; operation=" + a.getAclDetails().getOperation() + "; change=" + a.getAction());
            System.out.println("  Actual: " + (a.getAction() == PlanAction.ADD ? "ABSENT" : "PRESENT")
                    + "; desired: " + (a.getAction() == PlanAction.REMOVE ? "ABSENT" : "PRESENT"));
        });
        report.issues().stream().sorted(Comparator.comparing(i -> i.severity() == PlanningReport.Severity.WARNING ? 0 : 1)).forEach(i -> {
            System.out.println("[" + i.severity() + "] " + i.code() + " Resource: " + i.resource());
            System.out.println("  Actual: " + i.actual() + "; required: " + i.required() + "; state: " + i.state());
            System.out.println("  Reason: " + i.reason());
            System.out.println("  Action: " + i.action());
        });
        var count = PlanUtil.getOverview(report.changes(), noDelete, skipAcls, skipTopics);
        long changes = count.getAdd() + count.getUpdate() + count.getRemove();
        System.out.println("[SUMMARY]");
        System.out.println("  Proposed executable resource changes: " + changes);
        System.out.println("  Status: " + (report.blocked() ? "PLAN_BLOCKED" : changes == 0 ? "NO_CHANGES" : "PLAN_BUILT"));
        System.out.println("  Ready for apply: " + (report.blocked() ? "NO" : "FRESH_PREFLIGHT_REQUIRED"));
        if (report.blocked()) System.out.println("  Executable JSON: NOT_WRITTEN; any existing output file is unchanged.");
    }
}
