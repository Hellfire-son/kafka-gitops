package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot;
import com.devshawn.kafka.gitops.domain.plan.PartitionReplicaAssignment;
import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult;
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
    }

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
    private static final Comparator<TopicPartition> ORDER = Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    AdminReassignmentExecutor(Admin admin) {
        this(admin, new Settings(100, Duration.ofSeconds(10), Duration.ofMillis(100)), new TimeSource() {
            public long nanoTime() { return System.nanoTime(); }
            public void pause(long nanos) throws InterruptedException { TimeUnit.NANOSECONDS.sleep(nanos); }
        }, (session, topics, timeout) -> ReplicationMetadataReader.read(session, topics, timeout, true));
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

        Work(PreparedOperation operation, PartitionReplicaAssignment assignment) {
            this.operation = operation;
            this.assignment = assignment;
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
                if (work.values().stream().allMatch(w -> w.failed || w.state == State.COMPLETED)) break;
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

    private Observation observe(Map<TopicPartition, Work> work, long deadline) throws Exception {
        long rpc = rpcDeadline(deadline);
        DescribeClusterResult cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs(rpc)));
        String id = await(cluster.clusterId(), rpc);
        Map<String, TopicDescription> topics = await(admin.describeTopics(topicNames(work),
                new DescribeTopicsOptions().timeoutMs(timeoutMs(rpc))).allTopicNames(), rpc);
        Map<TopicPartition, PartitionReassignment> active = await(admin.listPartitionReassignments(
                new ListPartitionReassignmentsOptions().timeoutMs(timeoutMs(rpc))).reassignments(), rpc);
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
    private static ErrorCategory category(Throwable error) {
        Throwable cause = error;
        while (cause instanceof ExecutionException && cause.getCause() != null) cause = cause.getCause();
        if (cause instanceof Failure failure) return failure.category;
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
