// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.domain.plan.ApplyReport;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport.AclChange;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport.Result;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport.Step;
import com.devshawn.kafka.gitops.domain.plan.ApplyReport.TopicChange;
import com.devshawn.kafka.gitops.domain.plan.PlanningReport.TopicPreview;
import com.devshawn.kafka.gitops.domain.state.AclDetails;
import com.devshawn.kafka.gitops.enums.PlanAction;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.devshawn.kafka.gitops.domain.plan.ApplyReport.Result.*;

/** The summary counts resources by their recorded outcomes, including when execution stopped early. */
public final class ApplyReportRenderer {
    public enum Completion { COMPLETED, NO_CHANGES, FAILED }

    private record ResourceResult(PlanAction action, Result result) { }

    private ApplyReportRenderer() { }

    public static void print(ApplyReport report, Completion completion) {
        report.planning().ifPresent(planning -> PlanReportRenderer.printIssues(planning.issues()));
        if (completion == Completion.NO_CHANGES) {
            if (report.skipTopics() && report.skipAcls()) System.out.println("[APPLY] All resource scopes skipped.");
            else if (report.planning().isEmpty()) System.out.println("[APPLY] No changes required by the saved plan.");
            else System.out.println(report.skipTopics() || report.skipAcls()
                    ? "[APPLY] No changes required for checked resources." : "[APPLY] No changes required.");
        }

        List<ResourceResult> topics = new ArrayList<>();
        for (var change : report.topics()) {
            var plan = change.plan();
            boolean blocked = report.planningBlockers().contains(plan.getName());
            Result result = blocked ? BLOCKED : ApplyReport.combine(change.stepResults());
            var preview = report.planning().map(p -> p.topics().get(plan.getName())).orElse(null);
            printTopic(change, preview, result);
            topics.add(new ResourceResult(plan.getAction(), result));
        }
        for (String name : report.planningBlockers()) {
            if (report.topic(name) != null) continue;
            System.out.println("[APPLY] [BLOCKED] Topic: " + name);
            report.planning().map(p -> p.topics().get(name)).ifPresent(preview ->
                    PlanReportRenderer.printPreviewDetails(name, preview, false));
            printResult(BLOCKED);
            topics.add(new ResourceResult(PlanAction.UPDATE, BLOCKED));
        }

        Map<AclDetails, List<AclChange>> rules = new LinkedHashMap<>();
        for (AclChange acl : report.acls()) {
            ResourceOutput.acl("APPLY", acl.plan());
            printResult(acl.step().result());
            rules.computeIfAbsent(acl.plan().getAclDetails(), key -> new ArrayList<>()).add(acl);
        }
        var acls = rules.values().stream().map(rule -> new ResourceResult(rule.getLast().plan().getAction(),
                ApplyReport.combine(rule.stream().map(a -> a.step().result()).toList()))).toList();
        printSummary(report, completion, topics, acls);
    }

    private static void printTopic(TopicChange change, TopicPreview preview, Result result) {
        var topic = change.plan();
        ResourceOutput.heading("APPLY", topic.getAction(), "Topic", topic.getName());
        if (result != NOT_RUN) {
            switch (topic.getAction()) {
                case ADD -> ResourceOutput.topicCreation(topic.getTopicDetails().orElseThrow());
                case UPDATE -> printTopicUpdate(change, preview, result);
                case REMOVE -> ResourceOutput.topicDeletion();
                case NO_CHANGE -> { }
            }
        }
        printResult(result);
    }

    private static void printTopicUpdate(TopicChange change, TopicPreview preview, Result result) {
        var topic = change.plan();
        boolean multiple = change.stepResults().size() > 1;
        if (change.partitions() != null) printStep(ResourceOutput.partitions(topic.getPartitionPlan().orElseThrow()),
                change.partitions(), result, multiple);
        if (change.replication() != null) printReplication(change, result, multiple);
        for (var config : change.configs()) {
            String before = preview == null ? "UNKNOWN" : preview.currentConfigValues().getOrDefault(config.plan().getKey(), "UNKNOWN");
            printStep(ResourceOutput.config(config.plan(), before), config.step(), result, multiple);
        }
    }

    private static void printReplication(TopicChange change, Result result, boolean multiple) {
        var topic = change.plan();
        var rf = topic.getReplicationPlan().orElseThrow();
        var factors = new TreeSet<Integer>();
        rf.getAssignments().forEach(assignment -> factors.add(assignment.getPreviousReplicas().size()));
        printStep(ResourceOutput.replication(factors, "UNKNOWN", Integer.toString(rf.getTargetReplicationFactor())),
                change.replication(), result, multiple);
        var observed = change.replicationObservation();
        if (observed.isEmpty()) {
            System.out.println("  Preflight checks: " + (change.replication().result() == BLOCKED ? "BLOCKED" : "NOT_RUN"));
            return;
        }
        var replication = observed.get();
        var execution = replication.execution();
        System.out.println("  Preflight checks: PASSED");
        System.out.println("  Reassignment: " + execution.state());
        System.out.println("  Final verification: " + (replication.verified() ? "PASSED" : replication.verificationAttempted() ? "UNCONFIRMED" : "NOT_RUN"));
        for (var assignment : rf.getAssignments()) {
            var partition = execution.partitions().get(new TopicPartition(topic.getName(), assignment.getPartition()));
            String evidence = "";
            if (!replication.verified()) evidence = partition == null ? " (UNKNOWN)" : " (state=" + partition.state()
                    + ", submission=" + partition.submission() + ", last known=" + partition.lastKnownState()
                    + ", reason=" + partition.error() + ")";
            System.out.println("    Partition " + assignment.getPartition() + ": " + assignment.getPreviousReplicas()
                    + " -> " + assignment.getTargetReplicas() + evidence);
        }
    }

    private static void printStep(String line, Step step, Result result, boolean multiple) {
        System.out.println(line + (multiple || step.result() != result ? " [" + step.result() + "]" : ""));
    }

    private static void printResult(Result result) {
        System.out.println("  Result: " + result);
    }

    private static void printSummary(ApplyReport report, Completion completion, List<ResourceResult> topics, List<ResourceResult> acls) {
        List<ResourceResult> all = new ArrayList<>(topics);
        all.addAll(acls);
        long successful = count(all, SUCCESS);
        long partial = count(all, PARTIAL);
        long blocked = count(all, BLOCKED);
        long unknown = count(all, UNKNOWN);
        long deletedAcls = report.acls().stream().filter(AclChange::deleted).map(acl -> acl.plan().getAclDetails()).distinct().count();

        System.out.println("[SUMMARY]");
        System.out.println(report.skipTopics() ? "  Topics: SKIPPED" : "  Topics: " + successful(topics, PlanAction.ADD)
                + " created, " + successful(topics, PlanAction.UPDATE) + " updated, " + successful(topics, PlanAction.REMOVE) + " deleted");
        System.out.println(report.skipAcls() ? "  ACLs: SKIPPED" : "  ACLs: " + successful(acls, PlanAction.ADD)
                + " created, " + deletedAcls + " deleted");
        if (!report.skipTopics()) {
            if (report.planning().isPresent()) System.out.println("  Unchanged topics: " + report.unchangedEntries());
            else {
                System.out.println("  Unchanged topics: UNKNOWN");
                if (report.unchangedEntries() > 0) System.out.println("  Unchanged topics listed in JSON: " + report.unchangedEntries());
            }
        }
        System.out.println("  Successful resource changes: " + successful);
        System.out.println("  Failed resource changes: " + count(all, FAILED));
        printOptionalCount("Partially applied resources", partial);
        printOptionalCount("Blocked resources", blocked);
        printOptionalCount("Resources with unknown outcomes", unknown);
        printOptionalCount("Resources not run", count(all, NOT_RUN));
        printOptionalCount("Resources already satisfied", count(all, NO_OP));

        String status = switch (completion) {
            case COMPLETED -> "APPLY_COMPLETED";
            case NO_CHANGES -> "NO_CHANGES";
            case FAILED -> successful + partial > 0 ? "APPLY_PARTIAL" : unknown > 0 ? "APPLY_UNKNOWN"
                    : report.isBlocked() || blocked > 0 ? "APPLY_BLOCKED" : "APPLY_FAILED";
        };
        System.out.println("  Status: " + status);
    }

    private static long successful(List<ResourceResult> results, PlanAction action) {
        return results.stream().filter(r -> r.action() == action && r.result() == SUCCESS).count();
    }

    private static long count(List<ResourceResult> results, Result result) {
        return results.stream().filter(r -> r.result() == result).count();
    }

    private static void printOptionalCount(String label, long count) {
        if (count > 0) System.out.println("  " + label + ": " + count);
    }
}
