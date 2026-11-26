// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition;
import com.devshawn.kafka.gitops.domain.plan.PartitionReplicaAssignment;
import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult;
import com.devshawn.kafka.gitops.domain.plan.TopicReplicationPlan;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Internal only. The caller owns the supplied Admin. No production CLI binding or cancellation path. */
final class AdminReassignmentExecutor implements ReassignmentExecutor {
    interface TimeSource {
        long nanoTime();
        void pause(long nanos) throws InterruptedException;
    }

    @FunctionalInterface
    interface SnapshotReader {
        ReplicationClusterSnapshot read(Admin admin, Set<String> topics, int timeoutMs) throws Exception;

        default ReplicationClusterSnapshot readForVerification(Admin admin, Set<String> topics,
                                                               Map<String, TopicReplicationPlan> terminalPlans, AdminReadDeadline deadline)
                throws Exception {
            return read(admin, topics, deadline.timeoutMs());
        }
    }

    static final SnapshotReader LIVE_SNAPSHOTS = new SnapshotReader() {
        @Override public ReplicationClusterSnapshot read(Admin admin, Set<String> topics, int timeoutMs) throws Exception {
            return ReplicationMetadataReader.read(admin, topics, timeoutMs, true);
        }

        @Override public ReplicationClusterSnapshot readForVerification(Admin admin, Set<String> topics,
                                                                        Map<String, TopicReplicationPlan> terminalPlans,
                                                                        AdminReadDeadline deadline) throws Exception {
            return ReplicationMetadataReader.readForVerification(admin, topics, terminalPlans, deadline);
        }
    };

    record Settings(int batchSize, Duration rpcTimeout, Duration pollInterval) {
        Settings {
            if (batchSize < 1 || batchSize > 1000 || rpcTimeout == null || rpcTimeout.isNegative() || rpcTimeout.isZero()
                    || rpcTimeout.compareTo(Duration.ofMillis(Integer.MAX_VALUE)) > 0
                    || pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()
                    || pollInterval.compareTo(Duration.ofMinutes(1)) > 0) {
                throw new IllegalArgumentException("Invalid bounded reassignment settings.");
            }
        }
    }

    private final Admin admin;
    private final Settings settings;
    private final TimeSource time;
    private final SnapshotReader snapshots;
    private final ReplicationSafetyPolicy policy = new ReplicationSafetyPolicy();
    private static final Logger LOG = LoggerFactory.getLogger(AdminReassignmentExecutor.class);
    private static final Comparator<TopicPartition> ORDER = Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    AdminReassignmentExecutor(Admin admin) {
        this(admin, new Settings(100, Duration.ofSeconds(10), Duration.ofMillis(100)), new TimeSource() {
            public long nanoTime() { return System.nanoTime(); }
            public void pause(long nanos) throws InterruptedException { TimeUnit.NANOSECONDS.sleep(nanos); }
        }, LIVE_SNAPSHOTS);
    }

    /** Package-local injection for fake Admin, trusted test observations and monotonic test time. */
    AdminReassignmentExecutor(Admin admin, Settings settings, TimeSource time, SnapshotReader snapshots) {
        this.admin = Objects.requireNonNull(admin);
        this.settings = Objects.requireNonNull(settings);
        this.time = Objects.requireNonNull(time);
        this.snapshots = Objects.requireNonNull(snapshots);
    }

    private enum Position { ORIGINAL, TARGET_WAITING, TARGET_COMPLETE, ACTIVE, CONFLICT, STALE }
    private record Observation(String clusterId, Map<String, TopicDescription> topics,
                               Map<TopicPartition, PartitionReassignment> active) { }

    private static final class Work {
        final PreparedOperation operation;
        final PartitionReplicaAssignment assignment;
        State state = State.UNKNOWN;
        State lastKnown = State.UNKNOWN;
        ErrorCategory error = ErrorCategory.NOT_SUBMITTED;
        Submission submission = Submission.NOT_SENT;
        Position position = Position.ORIGINAL;
        boolean failed;
        boolean requireElr;

        Work(PreparedOperation operation, PartitionReplicaAssignment assignment) {
            this.operation = operation;
            this.assignment = assignment;
            this.requireElr = operation.preflight().elrMode()
                    == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED;
        }
        void known(State next) { state = next; lastKnown = next; error = ErrorCategory.NONE; }
        void fail(ErrorCategory category) { state = State.FAILED; error = category; failed = true; }
        boolean watching() { return !failed && submission != Submission.NOT_SENT && state != State.COMPLETED; }
        PartitionOutcome outcome() { return new PartitionOutcome(state, error, submission, lastKnown); }
    }

    @Override
    public ExecutionResult execute(Request request) {
        Objects.requireNonNull(request);
        Map<TopicPartition, Work> work = new TreeMap<>(ORDER);
        request.operations().forEach(o -> o.plan().getAssignments().forEach(a -> work.put(new TopicPartition(o.topicName(), a.getPartition()), new Work(o, a))));
        boolean stopWrites = false;
        try {
            if (time.nanoTime() - request.startedNanos() < 0) throw new Failure(ErrorCategory.VALIDATION);
            while (true) {
                requireTime(request.deadlineNanos());
                Observation observed = observe(work, request.deadlineNanos());
                stopWrites |= update(work, observed);
                if (stopWrites) stopPending(work);
                if (work.values().stream().allMatch(w -> w.failed || w.state == State.COMPLETED)) {
                    if (work.values().stream().allMatch(w -> w.state == State.COMPLETED)
                            && work.values().stream().anyMatch(w -> w.requireElr) && !verifyElrTerminal(request))
                        invalidate(work, ErrorCategory.PREFLIGHT_BLOCKED);
                    break;
                }
                if (work.values().stream().anyMatch(Work::watching)) {
                    pause(request.deadlineNanos());
                    continue;
                }
                if (stopWrites) break;

                // Every pending topic is revalidated before the next write. A cached READY is never authorization.
                long started = time.nanoTime();
                ReplicationClusterSnapshot snapshot = snapshots.read(admin, topicNames(work), timeoutMs(rpcDeadline(request.deadlineNanos())));
                long now = time.nanoTime();
                boolean ready = true;
                for (PreparedOperation operation : request.operations()) {
                    Set<Integer> completed = new HashSet<>();
                    work.forEach((key, value) -> {
                        if (key.topic().equals(operation.topicName()) && value.position == Position.TARGET_COMPLETE) completed.add(key.partition());
                    });
                    var result = policy.evaluateForExecution(operation.topicName(), operation.plan(), snapshot, request.permissions(),
                            new ReplicationSafetyPolicy.Constraints(false, false, started, now, Duration.ofSeconds(30)), completed);
                    ready &= result.status() == ReplicationSafetyResult.Status.READY;
                    var originalMode = operation.preflight().elrMode();
                    ready &= originalMode == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
                            || originalMode == snapshot.safety().elrMode();
                    if (snapshot.safety().elrMode() == com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.ENABLED)
                        work.values().stream().filter(w -> w.operation.topicName().equals(operation.topicName())).forEach(w -> w.requireElr = true);
                }
                if (!ready) {
                    work.values().stream().filter(w -> w.submission == Submission.NOT_SENT && w.state != State.COMPLETED)
                            .forEach(w -> w.fail(ErrorCategory.PREFLIGHT_BLOCKED));
                    break;
                }
                // Detect assignment/identity/ISR/leader drift after the full safety read. This still is not a CAS.
                Observation beforeWrite = observe(work, request.deadlineNanos());
                if (!sameSnapshot(snapshot, beforeWrite)) {
                    invalidate(work, ErrorCategory.STALE_PLAN);
                    break;
                }
                stopWrites |= update(work, beforeWrite);
                if (stopWrites || work.values().stream().anyMatch(Work::watching)) {
                    stopWrites = true;
                    stopPending(work);
                    continue;
                }
                List<Map.Entry<TopicPartition, Work>> batch = work.entrySet().stream()
                        .filter(e -> e.getValue().submission == Submission.NOT_SENT && e.getValue().position == Position.ORIGINAL)
                        .limit(settings.batchSize()).toList();
                if (batch.isEmpty()) break;
                stopWrites = !submit(batch, request.deadlineNanos());
                if (stopWrites) stopPending(work);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            uncertain(work, ErrorCategory.INTERRUPTED);
        } catch (Failure ex) {
            if (ex.category == ErrorCategory.STALE_PLAN) invalidate(work, ex.category);
            else uncertain(work, ex.category);
        } catch (Exception ex) {
            ErrorCategory category = category(ex);
            if (category == ErrorCategory.STALE_PLAN) invalidate(work, category);
            else uncertain(work, category);
        }
        Map<TopicPartition, PartitionOutcome> results = new TreeMap<>(ORDER);
        work.forEach((key, value) -> results.put(key, value.outcome()));
        return new ExecutionResult(work.keySet(), results);
    }

    /** Independent terminal read, used by the integration boundary before reporting CLI success. */
    boolean verifyCompleted(Request request) throws Exception {
        Map<TopicPartition, Work> work = new TreeMap<>(ORDER);
        request.operations().forEach(o -> o.plan().getAssignments().forEach(a -> work.put(new TopicPartition(o.topicName(), a.getPartition()), new Work(o, a))));
        // Metadata fallback can read a lagging broker immediately after another broker reports
        // completion. Only read again, within one RPC window and the original operation deadline.
        // No submission, target change or relaxation of the terminal proof takes place here.
        var verificationDeadline = AdminReadDeadline.forVerification(time::nanoTime, request.deadlineNanos(), settings.rpcTimeout().toNanos());
        while (true) {
            Observation observation = observe(work, verificationDeadline);
            boolean complete = true;
            TopicPartition waitingForRemovedIsr = null;
            for (var entry : work.entrySet()) {
                TopicPartition key = entry.getKey();
                Work item = entry.getValue();
                var p = partition(observation, key);
                item.requireElr |= p.elr() != null || p.lastKnownElr() != null;
                Set<Integer> replicas = new HashSet<>(p.replicas().stream().map(Node::id).toList());
                Set<Integer> isr = new HashSet<>(p.isr().stream().map(Node::id).toList());
                if (!replicas.containsAll(isr)) {
                    Partition sample = ElrSafetyPolicy.partition(p);
                    if (observation.active().containsKey(key)) return verificationFailed(key, "ACTIVE_REASSIGNMENT");
                    if (isr.size() != p.isr().size() || !removedReplicasInIsr(item.assignment, sample))
                        return verificationFailed(key, "ISR_OUTSIDE_ASSIGNMENT");
                    if (item.requireElr) {
                        var elr = ElrSafetyPolicy.observe(sample);
                        // The ISR mismatch is the reason to wait. ELR and LastKnownELR must still be proven empty.
                        if (elr.blocks() && elr.reason() != CapabilityAssessment.Reason.ELR_ISR_UNHEALTHY)
                            return verificationFailed(key, elr.reason().name());
                    }
                    waitingForRemovedIsr = key;
                    continue;
                }
                if (item.requireElr) {
                    var elr = ElrSafetyPolicy.observe(ElrSafetyPolicy.partition(p));
                    if (elr.blocks()) return verificationFailed(key, elr.reason().name());
                }
                Set<Integer> retained = new HashSet<>(item.assignment.getPreviousReplicas());
                retained.retainAll(item.assignment.getTargetReplicas());
                if (p.leader() == null || !replicas.contains(p.leader().id()) || !isr.contains(p.leader().id()))
                    return verificationFailed(key, "LEADER_NOT_IN_ISR");
                if (!isr.containsAll(retained)) return verificationFailed(key, "RETAINED_REPLICA_NOT_IN_ISR");
                Position position = classify(item.assignment, p, observation.active().get(key));
                if (position == Position.CONFLICT) return verificationFailed(key, "CONFLICTING_REASSIGNMENT");
                if (position == Position.STALE) return verificationFailed(key, "ASSIGNMENT_CHANGED");
                complete &= position == Position.TARGET_COMPLETE && isr.equals(replicas);
            }
            if (waitingForRemovedIsr != null) {
                if (!complete) return verificationFailed(waitingForRemovedIsr, "OTHER_PARTITIONS_NOT_AT_TARGET");
                return verifyRemovedIsrConvergence(request, work, verificationDeadline);
            }
            if (complete) {
                if (work.values().stream().anyMatch(w -> w.requireElr)) {
                    long started = verificationDeadline.now();
                    var snapshot = snapshots.readForVerification(admin, topicNames(work), Map.of(), verificationDeadline);
                    verificationDeadline.check();
                    if (!verifyElrTerminal(request, snapshot, started, verificationDeadline.now())) return false;
                }
                verificationDeadline.check();
                return true;
            }
            time.pause(Math.min(settings.pollInterval().toNanos(), verificationDeadline.remainingNanos()));
        }
    }

    /** An exact target with full target ISR and only known removed members still present. */
    static boolean removedReplicasInIsr(PartitionReplicaAssignment assignment, Partition actual) {
        List<Integer> previous = assignment.getPreviousReplicas();
        List<Integer> target = assignment.getTargetReplicas();
        return previous.size() > target.size() && previous.containsAll(target)
                && actual.replicas().equals(target) && actual.leader().isPresent() && target.contains(actual.leader().getAsInt())
                && actual.isr().size() > target.size() && actual.isr().containsAll(target) && previous.containsAll(actual.isr());
    }

    private boolean verifyRemovedIsrConvergence(Request request, Map<TopicPartition, Work> work, AdminReadDeadline deadline) throws Exception {
        Map<String, TopicReplicationPlan> plans = new TreeMap<>();
        request.operations().forEach(operation -> plans.put(operation.topicName(), operation.plan()));
        LOG.info("RF final verification observed removed replicas in the ISR and is checking for convergence.");
        try {
            while (true) {
                long started = deadline.now();
                deadline.check();
                var snapshot = snapshots.readForVerification(admin, plans.keySet(), plans, deadline);
                deadline.check();
                if (snapshot == null) return verificationFailed("METADATA_UNAVAILABLE");
                long now = deadline.now();
                boolean converged = true;
                for (var operation : request.operations()) {
                    var mode = snapshot.safety().elrMode();
                    if (operation.preflight().elrMode() != mode)
                        return verificationFailed("ELR_MODE_CHANGED; Topic: " + operation.topicName());
                    var topic = snapshot.topics().get(operation.topicName());
                    Map<Integer, Partition> waiting = new HashMap<>();
                    Set<Integer> completed = new HashSet<>();
                    for (var assignment : operation.plan().getAssignments()) {
                        int id = assignment.getPartition();
                        completed.add(id);
                        if (topic == null) continue; // The safety policy reports missing metadata below.
                        var actual = topic.partitions().stream().filter(p -> p.id() == id).findFirst().orElse(null);
                        if (actual == null) continue;
                        boolean transition = removedReplicasInIsr(assignment, actual);
                        var key = new TopicPartition(operation.topicName(), id);
                        Work item = work.get(key);
                        item.requireElr |= actual.elr().isPresent() || actual.lastKnownElr().isPresent();
                        if (item.requireElr) {
                            var elr = ElrSafetyPolicy.observe(actual);
                            if (elr.blocks() && !(transition && elr.reason() == CapabilityAssessment.Reason.ELR_ISR_UNHEALTHY))
                                return verificationFailed(key, elr.reason().name());
                        }
                        if (transition) waiting.put(id, actual);
                    }
                    var safety = terminalSafety(request, operation, snapshot, started, now, completed);
                    var blockers = safety.reasons().stream().filter(finding -> {
                        Partition transition = waiting.get(finding.partition().orElse(-1));
                        if (transition == null) return true;
                        // Only the already proven excess ISR can be pending. Neither finding can authorize success.
                        return finding.code() != ReplicationSafetyResult.Reason.ISR_INCOMPLETE
                                && (finding.code() != ReplicationSafetyResult.Reason.ELR_PARTITION_UNSAFE
                                || ElrSafetyPolicy.assess(mode, transition).reason() != CapabilityAssessment.Reason.ELR_ISR_UNHEALTHY);
                    }).map(f -> f.code().name()).distinct().sorted().toList();
                    if (!blockers.isEmpty())
                        return verificationFailed("Topic: " + operation.topicName() + "; reasons=" + blockers);
                    converged &= waiting.isEmpty();
                }
                deadline.check();
                if (converged) return true;
                time.pause(Math.min(settings.pollInterval().toNanos(), deadline.remainingNanos()));
            }
        } catch (AdminReadDeadline.Exhausted ex) {
            verificationFailed("METADATA_NOT_CONVERGED; could not confirm the target ISR within the verification deadline");
            throw ex;
        } catch (KafkaExecutionException ex) {
            if (ex.getCode() == KafkaExecutionException.Code.METADATA_INVALID)
                return verificationFailed(ex.getCode().name());
            throw ex;
        }
    }

    private static boolean verificationFailed(TopicPartition partition, String reason) {
        return verificationFailed("Topic: " + partition.topic() + "; partition: " + partition.partition() + "; reason=" + reason);
    }

    private static boolean verificationFailed(String reason) {
        System.out.println("[INVALID] RF terminal verification; " + reason + ".");
        return false;
    }

    /** Fresh feature/config/broker/ELR evidence at completion, not just the earlier successful RPC. */
    private boolean verifyElrTerminal(Request request) throws Exception {
        long started = time.nanoTime();
        Set<String> topics = new TreeSet<>(); request.operations().forEach(o -> topics.add(o.topicName()));
        var snapshot = snapshots.read(admin, topics, timeoutMs(rpcDeadline(request.deadlineNanos())));
        return verifyElrTerminal(request, snapshot, started, time.nanoTime());
    }

    private boolean verifyElrTerminal(Request request, ReplicationClusterSnapshot snapshot, long started, long now) {
        for (var operation : request.operations()) {
            var originalMode = operation.preflight().elrMode();
            if (originalMode != com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
                    && originalMode != snapshot.safety().elrMode()) {
                System.out.println("[INVALID] RF terminal verification: ELR_MODE_CHANGED; Topic: " + operation.topicName());
                return false;
            }
            Set<Integer> completed = new HashSet<>(); operation.plan().getAssignments().forEach(a -> completed.add(a.getPartition()));
            var result = terminalSafety(request, operation, snapshot, started, now, completed);
            if (result.status() != ReplicationSafetyResult.Status.READY) {
                System.out.println("[INVALID] RF terminal verification; Topic: " + operation.topicName()
                        + "; reasons=" + result.reasons().stream().map(f -> f.code().name()).distinct().sorted().toList());
                return false;
            }
        }
        return true;
    }

    private ReplicationSafetyResult terminalSafety(Request request, PreparedOperation operation, ReplicationClusterSnapshot snapshot,
                                                   long started, long now, Set<Integer> completed) {
        return policy.evaluateForExecution(operation.topicName(), operation.plan(), snapshot, request.permissions(),
                new ReplicationSafetyPolicy.Constraints(false, false, started, now, Duration.ofSeconds(30)), completed);
    }

    private Observation observe(Map<TopicPartition, Work> work, long deadline) throws Exception {
        long rpc = rpcDeadline(deadline);
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs(rpc)));
        String id = await(cluster.clusterId(), rpc);
        Map<String, TopicDescription> topics = await(admin.describeTopics(topicNames(work),
                new DescribeTopicsOptions().timeoutMs(timeoutMs(rpc))).allTopicNames(), rpc);
        Map<TopicPartition, PartitionReassignment> active = await(admin.listPartitionReassignments(
                new ListPartitionReassignmentsOptions().timeoutMs(timeoutMs(rpc))).reassignments(), rpc);
        return checkedObservation(work, id, topics, active);
    }

    private Observation observe(Map<TopicPartition, Work> work, AdminReadDeadline deadline) throws Exception {
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(deadline.timeoutMs()));
        String id = deadline.await(cluster.clusterId());
        Map<String, TopicDescription> topics = deadline.await(admin.describeTopics(topicNames(work),
                new DescribeTopicsOptions().timeoutMs(deadline.timeoutMs())).allTopicNames());
        Map<TopicPartition, PartitionReassignment> active = deadline.await(admin.listPartitionReassignments(
                new ListPartitionReassignmentsOptions().timeoutMs(deadline.timeoutMs())).reassignments());
        return checkedObservation(work, id, topics, active);
    }

    private static Observation checkedObservation(Map<TopicPartition, Work> work, String id, Map<String, TopicDescription> topics,
                                                  Map<TopicPartition, PartitionReassignment> active) throws Failure {
        for (PreparedOperation operation : work.values().stream().map(w -> w.operation).distinct().toList()) {
            TopicDescription topic = topics.get(operation.topicName());
            if (!operation.plan().getClusterId().equals(id) || topic == null
                    || !operation.plan().getTopicId().equals(topic.topicId().toString())
                    || topic.partitions().size() != operation.plan().getPartitionCount()) throw new Failure(ErrorCategory.STALE_PLAN);
            Set<Integer> ids = new HashSet<>();
            for (TopicPartitionInfo partition : topic.partitions()) {
                if (partition.partition() < 0 || partition.partition() >= operation.plan().getPartitionCount() || !ids.add(partition.partition())) throw new Failure(ErrorCategory.STALE_PLAN);
            }
        }
        return new Observation(id, topics, active);
    }

    private boolean update(Map<TopicPartition, Work> work, Observation observed) {
        boolean stop = false;
        for (var entry : work.entrySet()) {
            Work item = entry.getValue();
            if (item.failed) continue;
            TopicPartitionInfo actual = partition(observed, entry.getKey());
            item.requireElr |= actual.elr() != null || actual.lastKnownElr() != null;
            if (item.requireElr) {
                var elr = ElrSafetyPolicy.observe(ElrSafetyPolicy.partition(actual));
                if (elr.blocks()) {
                    boolean unknown = elr.state() == com.devshawn.kafka.gitops.domain.metadata.CapabilityAssessment.State.UNKNOWN;
                    item.error = unknown ? ErrorCategory.ELR_UNKNOWN : ErrorCategory.ELR_UNSAFE;
                    item.state = unknown ? State.UNKNOWN : State.FAILED;
                    item.failed = true; stop = true;
                    continue;
                }
                Set<Integer> retained = new HashSet<>(item.assignment.getPreviousReplicas());
                retained.retainAll(item.assignment.getTargetReplicas());
                if (!new HashSet<>(actual.isr().stream().map(Node::id).toList()).containsAll(retained)) {
                    item.fail(ErrorCategory.ELR_UNSAFE); stop = true;
                    continue;
                }
            }
            Position position = classify(item.assignment, actual, observed.active().get(entry.getKey()));
            item.position = position;
            switch (position) {
                case TARGET_COMPLETE -> {
                    if (item.submission == Submission.NOT_SENT) item.submission = Submission.OBSERVED;
                    item.known(State.COMPLETED);
                }
                case TARGET_WAITING, ACTIVE -> {
                    if (item.submission == Submission.NOT_SENT) item.submission = Submission.OBSERVED;
                    item.known(State.RUNNING);
                }
                case ORIGINAL -> {
                    if (item.submission == Submission.OBSERVED || item.state == State.COMPLETED) {
                        item.fail(ErrorCategory.STALE_PLAN); stop = true;
                    }
                    // A SENT request may be accepted while metadata is still original. Never resubmit it.
                }
                case CONFLICT -> { item.fail(ErrorCategory.CONFLICT); stop = true; }
                case STALE -> { item.fail(ErrorCategory.STALE_PLAN); stop = true; }
            }
        }
        return stop;
    }

    private static Position classify(PartitionReplicaAssignment assignment, TopicPartitionInfo actual, PartitionReassignment active) {
        List<Integer> replicas = actual.replicas().stream().map(Node::id).toList();
        List<Integer> previous = assignment.getPreviousReplicas();
        List<Integer> target = assignment.getTargetReplicas();
        if (active != null) {
            Set<Integer> adding = new HashSet<>(target); adding.removeAll(previous);
            Set<Integer> removing = new HashSet<>(previous); removing.removeAll(target);
            Set<Integer> union = new HashSet<>(previous); union.addAll(target);
            boolean compatible = unique(active.replicas()) && unique(active.addingReplicas()) && unique(active.removingReplicas())
                    && new HashSet<>(active.replicas()).equals(union)
                    && new HashSet<>(active.addingReplicas()).equals(adding) && new HashSet<>(active.removingReplicas()).equals(removing)
                    && active.replicas().stream().filter(id -> !removing.contains(id)).toList().equals(target)
                    && (replicas.equals(previous) || replicas.equals(target) || replicas.equals(active.replicas()));
            return compatible ? Position.ACTIVE : Position.CONFLICT;
        }
        if (replicas.equals(target)) {
            Set<Integer> isr = new HashSet<>(actual.isr().stream().map(Node::id).toList());
            return isr.containsAll(target) && actual.leader() != null && target.contains(actual.leader().id())
                    && isr.contains(actual.leader().id()) ? Position.TARGET_COMPLETE : Position.TARGET_WAITING;
        }
        return replicas.equals(previous) ? Position.ORIGINAL : Position.STALE;
    }

    private boolean submit(List<Map.Entry<TopicPartition, Work>> batch, long deadline) throws InterruptedException, Failure {
        long rpc = rpcDeadline(deadline);
        Map<TopicPartition, Optional<NewPartitionReassignment>> assignments = new LinkedHashMap<>();
        for (var entry : batch) assignments.put(entry.getKey(), Optional.of(new NewPartitionReassignment(entry.getValue().assignment.getTargetReplicas())));
        Map<TopicPartition, KafkaFuture<Void>> futures;
        AlterPartitionReassignmentsOptions options = new AlterPartitionReassignmentsOptions().timeoutMs(timeoutMs(rpc));
        for (var entry : batch) { entry.getValue().submission = Submission.SENT; entry.getValue().error = ErrorCategory.UNKNOWN; }
        try {
            futures = admin.alterPartitionReassignments(assignments, options).values();
        } catch (Exception ex) {
            if (ex instanceof InterruptedException interrupted) throw interrupted;
            for (var entry : batch) rejectOrUnknown(entry.getValue(), ex);
            return false;
        }
        boolean accepted = true;
        for (var entry : batch) {
            Work item = entry.getValue();
            KafkaFuture<Void> future = futures.get(entry.getKey());
            if (future == null) { item.error = ErrorCategory.UNKNOWN; accepted = false; continue; }
            try {
                // Process already-completed siblings even after another partition consumes the RPC timeout.
                future.get(Math.max(0, rpc - time.nanoTime()), TimeUnit.NANOSECONDS);
                item.known(State.ACCEPTED);
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                rejectOrUnknown(item, ex);
                accepted = false;
            }
        }
        return accepted;
    }

    private static void rejectOrUnknown(Work item, Exception error) {
        ErrorCategory category = category(error);
        if (category == ErrorCategory.AUTHORIZATION || category == ErrorCategory.VALIDATION || category == ErrorCategory.UNSUPPORTED
                || category == ErrorCategory.STALE_PLAN) item.fail(category);
        else { item.state = State.UNKNOWN; item.error = category; }
    }

    private static boolean sameSnapshot(ReplicationClusterSnapshot snapshot, Observation observation) {
        if (snapshot == null || !Objects.equals(snapshot.clusterId(), observation.clusterId())) return false;
        Set<TopicPartition> expectedActive = new HashSet<>();
        snapshot.reassignments().stream().filter(p -> snapshot.topics().containsKey(p.topic())).forEach(expectedActive::add);
        Set<TopicPartition> actualActive = new HashSet<>();
        observation.active().keySet().stream().filter(p -> snapshot.topics().containsKey(p.topic())).forEach(actualActive::add);
        if (!expectedActive.equals(actualActive)) return false;
        for (var entry : snapshot.topics().entrySet()) {
            TopicDescription actual = observation.topics().get(entry.getKey());
            var expected = entry.getValue();
            if (actual == null || !expected.topicId().equals(actual.topicId().toString()) || expected.partitions().size() != actual.partitions().size()) return false;
            for (var part : expected.partitions()) {
                TopicPartitionInfo p = actual.partitions().stream().filter(a -> a.partition() == part.id()).findFirst().orElse(null);
                if (p == null || !part.replicas().equals(p.replicas().stream().map(Node::id).toList())
                        || !part.isr().equals(new HashSet<>(p.isr().stream().map(Node::id).toList()))
                        || !part.elr().equals(ElrSafetyPolicy.partition(p).elr())
                        || !part.lastKnownElr().equals(ElrSafetyPolicy.partition(p).lastKnownElr())
                        || part.leader().isPresent() != (p.leader() != null && p.leader().id() >= 0)
                        || (part.leader().isPresent() && part.leader().getAsInt() != p.leader().id())) return false;
            }
        }
        return true;
    }

    private static boolean unique(List<Integer> ids) { return ids.stream().allMatch(id -> id != null && id >= 0) && new HashSet<>(ids).size() == ids.size(); }
    private static TopicPartitionInfo partition(Observation observed, TopicPartition key) {
        return observed.topics().get(key.topic()).partitions().stream().filter(p -> p.partition() == key.partition()).findFirst().orElseThrow();
    }
    private static Set<String> topicNames(Map<TopicPartition, Work> work) {
        Set<String> topics = new TreeSet<>(); work.keySet().forEach(k -> topics.add(k.topic())); return topics;
    }
    private static void stopPending(Map<TopicPartition, Work> work) {
        work.values().stream().filter(w -> w.submission == Submission.NOT_SENT && w.state != State.COMPLETED && !w.failed).forEach(w -> w.fail(ErrorCategory.NOT_SUBMITTED));
    }
    private static void invalidate(Map<TopicPartition, Work> work, ErrorCategory category) {
        work.values().stream().filter(w -> !w.failed).forEach(w -> {
            if (w.submission == Submission.SENT) { w.state = State.UNKNOWN; w.error = category; }
            else w.fail(category);
        });
    }
    private static void uncertain(Map<TopicPartition, Work> work, ErrorCategory category) {
        work.values().stream().filter(w -> !w.failed && w.state != State.COMPLETED).forEach(w -> { w.state = State.UNKNOWN; w.error = category; });
    }
    static ErrorCategory category(Throwable error) {
        Throwable cause = error;
        while (cause instanceof ExecutionException && cause.getCause() != null) cause = cause.getCause();
        if (cause instanceof Failure failure) return failure.category;
        if (cause instanceof AdminReadDeadline.Exhausted) return ErrorCategory.DEADLINE_EXCEEDED;
        if (cause instanceof TimeoutException || cause instanceof org.apache.kafka.common.errors.TimeoutException) return ErrorCategory.RPC_TIMEOUT;
        if (cause instanceof InterruptedException) return ErrorCategory.INTERRUPTED;
        if (cause instanceof AuthorizationException) return ErrorCategory.AUTHORIZATION;
        if (cause instanceof InvalidReplicaAssignmentException || cause instanceof IllegalArgumentException) return ErrorCategory.VALIDATION;
        if (cause instanceof UnsupportedVersionException) return ErrorCategory.UNSUPPORTED;
        if (cause instanceof UnknownTopicOrPartitionException) return ErrorCategory.STALE_PLAN;
        if (cause instanceof NetworkException) return ErrorCategory.BROKER_UNAVAILABLE;
        return ErrorCategory.INTERNAL;
    }
    private long rpcDeadline(long deadline) throws Failure { requireTime(deadline); return time.nanoTime() + Math.min(settings.rpcTimeout().toNanos(), deadline - time.nanoTime()); }
    private int timeoutMs(long deadline) throws Failure { requireTime(deadline); return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - time.nanoTime())); }
    private void requireTime(long deadline) throws Failure {
        if (Thread.currentThread().isInterrupted()) throw new Failure(ErrorCategory.INTERRUPTED);
        if (deadline - time.nanoTime() <= 0) throw new Failure(ErrorCategory.DEADLINE_EXCEEDED);
    }
    private <T> T await(KafkaFuture<T> future, long deadline) throws Exception { requireTime(deadline); return future.get(deadline - time.nanoTime(), TimeUnit.NANOSECONDS); }
    private void pause(long deadline) throws InterruptedException, Failure { requireTime(deadline); time.pause(Math.min(settings.pollInterval().toNanos(), deadline - time.nanoTime())); }
    private static final class Failure extends Exception {
        final ErrorCategory category;
        Failure(ErrorCategory category) { this.category = category; }
    }
}
