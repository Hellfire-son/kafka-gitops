// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.DesiredPlan;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport.Issue;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport.Severity;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport.TopicPreview;
import com.devshawn.kafka.gitops.domain.plan.TopicPlan;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.enums.PlanAction;
import com.devshawn.kafka.gitops.service.ElrSafetyPolicy;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/** Render proposed changes, relevant diagnostics, and the command outcome. */
public final class PlanReportRenderer {
    private PlanReportRenderer() { }

    public static void print(PlanningReport report, boolean noDelete, boolean skipAcls, boolean skipTopics) {
        Map<String, Severity> topicIssues = new HashMap<>();
        report.issues().forEach(issue -> issue.topic().ifPresent(name ->
                topicIssues.merge(name, issue.severity(), (first, second) -> first == Severity.BLOCKED ? first : second)));

        if (!skipTopics) printTopics(report, noDelete, topicIssues);
        if (!skipAcls) printAcls(report.changes(), noDelete);
        printIssues(report.issues());
        printSummary(report, noDelete, skipAcls, skipTopics, topicIssues);
    }

    static void printIssues(List<Issue> issues) {
        issues.stream().sorted(Comparator.comparing(Issue::severity)).forEach(issue -> {
            System.out.println("[" + issue.severity() + "] " + issue.code() + " Resource: " + issue.resource());
            System.out.println("  Actual: " + issue.actual() + "; required: " + issue.required() + "; state: " + issue.state());
            System.out.println("  Reason: " + issue.reason());
            System.out.println("  Action: " + issue.action());
        });
    }

    private static void printTopics(PlanningReport report, boolean noDelete, Map<String, Severity> topicIssues) {
        Map<String, TopicPlan> planned = new HashMap<>();
        report.changes().getTopicPlans().forEach(topic -> planned.put(topic.getName(), topic));
        boolean blocked = report.blocked();

        report.topics().forEach((name, preview) -> {
            TopicPlan topic = planned.get(name);
            Severity severity = topicIssues.get(name);
            if (topic == null) {
                if (blocked || severity != null) printTopicPreview(name, preview, severity, true);
            } else if (topic.getAction() == PlanAction.ADD) {
                ResourceOutput.heading("PLAN", topic.getAction(), "Topic", name);
                ResourceOutput.topicCreation(topic.getTopicDetails().orElseThrow());
            } else if (topic.getAction() == PlanAction.UPDATE) {
                ResourceOutput.heading("PLAN", topic.getAction(), "Topic", name);
                printTopicUpdate(topic, preview, severity != null);
            } else if (topic.getAction() == PlanAction.NO_CHANGE && severity != null) {
                printTopicPreview(name, preview, severity, false);
            }
        });

        if (!noDelete) report.changes().getTopicPlans().stream().filter(t -> t.getAction() == PlanAction.REMOVE).forEach(t -> {
            ResourceOutput.heading("PLAN", t.getAction(), "Topic", t.getName());
            ResourceOutput.topicDeletion();
        });
    }

    private static void printTopicUpdate(TopicPlan topic, TopicPreview preview, boolean hasIssue) {
        if (topic.getPartitionPlan().isPresent()) {
            System.out.println(ResourceOutput.partitions(topic.getPartitionPlan().get()));
        } else if (topic.getReplicationPlan().isPresent() || hasIssue) {
            printPartitionPreview(preview);
        }
        if (topic.getReplicationPlan().isPresent() || hasIssue) {
            printReplication(topic.getName(), preview, topic.getReplicationPlan());
        }

        topic.getTopicConfigPlans().stream().filter(c -> c.getAction() != PlanAction.NO_CHANGE).forEach(config ->
                System.out.println(ResourceOutput.config(config, preview.currentConfigValues().getOrDefault(config.getKey(), "UNKNOWN"))));
    }

    private static void printTopicPreview(String name, TopicPreview preview, Severity severity, boolean unplanned) {
        System.out.println("[PLAN] [" + (severity == Severity.BLOCKED ? "BLOCKED" : "PREVIEW") + "] Topic: " + name);
        printPreviewDetails(name, preview, unplanned);
    }

    static void printPreviewDetails(String name, TopicPreview preview, boolean unplanned) {
        printPartitionPreview(preview);
        printReplication(name, preview, Optional.empty());
        if (unplanned) new TreeMap<>(preview.desired().getConfigs()).forEach((key, value) -> {
            boolean sensitive = TopicConfigSecurity.isSensitive(key) || preview.sensitiveConfigKeys().contains(key);
            String before = sensitive ? "[redacted]" : preview.currentConfigValues().getOrDefault(key, missingState(preview));
            String after = sensitive ? "[redacted]" : value;
            System.out.println("  Config " + key + ": " + before + " -> " + after + " [PREVIEW_ONLY]");
        });
    }

    private static void printPartitionPreview(TopicPreview preview) {
        String before = preview.current().map(t -> Integer.toString(t.partitions().size())).orElse(missingState(preview));
        System.out.println("  Partitions: " + before + " -> " + preview.desired().getPartitions());
    }

    private static String missingState(TopicPreview preview) {
        return preview.existenceKnown() && !preview.exists() ? "ABSENT" : "UNKNOWN";
    }

    private static void printReplication(String name, TopicPreview preview, Optional<TopicReplicationPlan> proposed) {
        var actualFactors = new TreeSet<Integer>();
        preview.current().ifPresent(t -> t.partitions().forEach(p -> actualFactors.add(p.replicas().size())));
        var target = proposed.map(TopicReplicationPlan::getTargetReplicationFactor).or(preview.desired()::getReplication);
        System.out.println(ResourceOutput.replication(actualFactors, missingState(preview), target.map(Object::toString).orElse("UNKNOWN")));

        boolean rfDiffers = target.map(rf -> actualFactors.stream().anyMatch(actual -> !actual.equals(rf))).orElse(false);
        if (proposed.isPresent() || rfDiffers || preview.replication().isPresent()) preview.current().ifPresent(t -> {
            System.out.println("  Current ordered assignments:");
            t.partitions().stream().sorted(Comparator.comparingInt(TopicPartitionInfo::partition)).forEach(p ->
                    System.out.println("    Partition " + p.partition() + ": " + p.replicas().stream().map(n -> n.id()).toList()
                            + "; leader=" + (p.leader() == null ? "UNKNOWN" : p.leader().id()) + "; "
                            + ElrSafetyPolicy.actual(ElrSafetyPolicy.partition(p))));
        });
        preview.replication().ifPresent(snapshot -> {
            System.out.println("  ELR feature: " + snapshot.safety().elrMode());
            var observed = snapshot.topics().get(name);
            if (observed != null) observed.partitions().forEach(p -> {
                var assessment = ElrSafetyPolicy.assess(snapshot.safety().elrMode(), p);
                System.out.println("  ELR safety partition " + p.id() + ": " + assessment.state() + " (" + assessment.reason()
                        + "); " + ElrSafetyPolicy.actual(p));
            });
        });
        if (proposed.isPresent()) {
            System.out.println("  Proposed ordered assignments (subject to fresh apply preflight):");
            proposed.get().getAssignments().forEach(a ->
                    System.out.println("    Partition " + a.getPartition() + ": " + a.getTargetReplicas()));
        } else if (rfDiffers) {
            System.out.println("  Proposed assignments: NOT_CALCULATED (see diagnostics)");
        }
        preview.brokers().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            var broker = entry.getValue();
            System.out.println("  Broker " + entry.getKey() + ": " + (broker.isPreviewCandidate() && broker.fencingEstablished() ? "ELIGIBLE_FOR_PREVIEW" : "EXCLUDED")
                    + ", rack=" + broker.rack().orElse("UNKNOWN") + ", storage=" + broker.storage()
                    + ", fencing=" + broker.unfenced() + ", cordoning=" + broker.uncordoned()
                    + ", usableBytes=" + (broker.minimumUsableBytes().isPresent() ? broker.minimumUsableBytes().getAsLong() : "UNKNOWN"));
            broker.capabilities().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(c ->
                    System.out.println("    " + c.getKey() + ": " + c.getValue().state() + " (" + c.getValue().reason() + ")"));
        });
    }

    private static void printAcls(DesiredPlan plan, boolean noDelete) {
        plan.getAclPlans().forEach(acl -> {
            var action = acl.getAction();
            if (action == PlanAction.ADD || (action == PlanAction.REMOVE && !noDelete)) {
                ResourceOutput.acl("PLAN", acl);
            }
        });
    }

    private static void printSummary(PlanningReport report, boolean noDelete, boolean skipAcls, boolean skipTopics,
                                     Map<String, Severity> topicIssues) {
        var plan = report.changes();
        var count = PlanUtil.getOverview(plan, noDelete, skipAcls, skipTopics);
        long changes = count.getAdd() + count.getUpdate() + count.getRemove();
        boolean blocked = report.blocked();
        if (!blocked && changes == 0 && !(skipTopics && skipAcls)) {
            System.out.println(skipTopics || skipAcls ? "[PLAN] No changes required for checked resources." : "[PLAN] No changes required.");
        }

        System.out.println("[SUMMARY]");
        if (skipTopics) {
            System.out.println("  Topics: SKIPPED");
        } else {
            var topics = PlanUtil.getTopicPlanOverview(plan, noDelete);
            System.out.println("  Topics: " + topics.getAdd() + " to create, " + topics.getUpdate() + " to update, " + topics.getRemove() + " to delete");
        }
        if (skipAcls) {
            System.out.println("  ACLs: SKIPPED");
        } else {
            var acls = PlanUtil.getAclPlanOverview(plan, noDelete);
            System.out.println("  ACLs: " + acls.getAdd() + " to create, " + acls.getRemove() + " to delete");
        }
        if (!skipTopics) {
            long unchanged = plan.getTopicPlans().stream().filter(topic -> topic.getAction() == PlanAction.NO_CHANGE
                    && topicIssues.get(topic.getName()) != Severity.BLOCKED).count();
            System.out.println("  Unchanged topics: " + unchanged);
            if (blocked) {
                long blockedTopics = topicIssues.values().stream().filter(severity -> severity == Severity.BLOCKED).count();
                System.out.println("  Blocked topics: " + blockedTopics);
                long withProposals = plan.getTopicPlans().stream()
                        .filter(topic -> topicIssues.get(topic.getName()) == Severity.BLOCKED && topic.getAction() != PlanAction.NO_CHANGE
                                && (topic.getAction() != PlanAction.REMOVE || !noDelete))
                        .map(TopicPlan::getName).distinct().count();
                if (withProposals > 0) System.out.println("  Blocked topics with other proposals: " + withProposals);
            }
        }
        System.out.println("  Total resource changes: " + changes + (blocked ? " (proposed, plan blocked)" : ""));
        if (blocked) System.out.println("  Blocking diagnostics: " + report.issues().stream().filter(i -> i.severity() == Severity.BLOCKED).count());
        System.out.println("  Status: " + (blocked ? "PLAN_BLOCKED" : changes == 0 ? "NO_CHANGES" : "PLAN_BUILT"));
        System.out.println("  Ready for apply: " + (blocked ? "NO" : "FRESH_PREFLIGHT_REQUIRED"));
        if (blocked) System.out.println("  Executable JSON: NOT_WRITTEN (existing output file preserved).");
    }
}
