// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.State.*;
import static com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.Reason.*;

/** Conservative ELR policy shared by preview, fresh preflight and terminal observation. No IO or permissions. */
public final class ElrSafetyPolicy {
    private ElrSafetyPolicy() { }

    public static Partition partition(TopicPartitionInfo p) {
        return new Partition(p.partition(), p.replicas().stream().map(Node::id).toList(),
                p.leader() == null || p.leader().id() < 0 ? OptionalInt.empty() : OptionalInt.of(p.leader().id()),
                new HashSet<>(p.isr().stream().map(Node::id).toList()), ids(p.elr()), ids(p.lastKnownElr()));
    }

    private static Optional<List<Integer>> ids(List<Node> nodes) {
        // Client 4.4 Metadata fallback uses the four-argument constructor and null ELR lists.
        // DescribeTopicPartitions carries nullable wire arrays. Absence never becomes empty.
        return nodes == null ? Optional.empty() : Optional.of(nodes.stream().map(n -> n == null ? -1 : n.id()).toList());
    }

    public static CapabilityAssessment assess(ElrMode mode, Partition p) {
        return assess(mode, p, true);
    }

    // During a known running reassignment the target ISR is allowed to be catching up.
    // ELR evidence must still be present and empty; this does not authorize another write.
    static CapabilityAssessment observe(Partition p) { return assess(ElrMode.ENABLED, p, false); }

    private static CapabilityAssessment assess(ElrMode mode, Partition p, boolean requireFullIsr) {
        if (mode == null || mode == ElrMode.UNKNOWN) return assessment(UNKNOWN, ELR_MODE_UNESTABLISHED);
        if (p.elr().stream().anyMatch(values -> !valid(values, p))
                || p.lastKnownElr().stream().anyMatch(values -> !valid(values, p)))
            return assessment(UNSAFE, ELR_METADATA_INVALID);
        if (p.elr().stream().anyMatch(values -> !values.isEmpty())) return assessment(UNSAFE, ELR_NONEMPTY);
        if (p.lastKnownElr().stream().anyMatch(values -> !values.isEmpty())) return assessment(UNSAFE, LAST_KNOWN_ELR_NONEMPTY);
        if (mode == ElrMode.NOT_APPLICABLE) return assessment(NOT_APPLICABLE, ELR_NOT_APPLICABLE);
        if (mode == ElrMode.DISABLED) return assessment(SUPPORTED, ELR_DISABLED);
        if (p.elr().isEmpty() || p.lastKnownElr().isEmpty()) return assessment(UNKNOWN, ELR_METADATA_UNAVAILABLE);
        if (p.replicas().isEmpty() || p.leader().isEmpty() || !p.replicas().contains(p.leader().getAsInt())
                || !p.isr().contains(p.leader().getAsInt()) || !new HashSet<>(p.replicas()).containsAll(p.isr())
                || requireFullIsr && !p.isr().equals(new HashSet<>(p.replicas()))) return assessment(UNSAFE, ELR_ISR_UNHEALTHY);
        return assessment(SUPPORTED, ELR_VERIFIED_EMPTY);
    }

    private static boolean valid(List<Integer> values, Partition p) {
        return values.stream().allMatch(id -> id != null && id >= 0 && p.replicas().contains(id))
                && new HashSet<>(values).size() == values.size();
    }

    private static CapabilityAssessment assessment(CapabilityAssessment.State state, CapabilityAssessment.Reason reason) {
        return new CapabilityAssessment(state, reason, true);
    }

    public static String actual(Partition p) {
        return "ISR=" + new TreeSet<>(p.isr()) + ", ELR=" + p.elr().map(Object::toString).orElse("UNKNOWN")
                + ", LastKnownELR=" + p.lastKnownElr().map(Object::toString).orElse("UNKNOWN");
    }
}
