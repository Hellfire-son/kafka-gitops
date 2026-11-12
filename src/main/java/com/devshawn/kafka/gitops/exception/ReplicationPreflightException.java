package com.devshawn.kafka.gitops.exception;

import com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class ReplicationPreflightException extends ValidationException {
    public enum Outcome { BLOCKED, EXECUTION_NOT_IMPLEMENTED }

    private final Outcome outcome;
    private final Map<String, ReplicationSafetyResult> results;

    public ReplicationPreflightException(Map<String, ReplicationSafetyResult> results) {
        super(message(results));
        this.results = Collections.unmodifiableMap(new TreeMap<>(results));
        this.outcome = results.values().stream().anyMatch(r -> r.status() == ReplicationSafetyResult.Status.BLOCKED)
                ? Outcome.BLOCKED : Outcome.EXECUTION_NOT_IMPLEMENTED;
    }

    public Outcome getOutcome() { return outcome; }
    public Map<String, ReplicationSafetyResult> getResults() { return results; }

    private static String message(Map<String, ReplicationSafetyResult> results) {
        String codes = results.values().stream().flatMap(r -> r.reasons().stream()).map(f -> f.code().name())
                .distinct().sorted().collect(Collectors.joining(", "));
        String details = results.entrySet().stream().flatMap(entry -> entry.getValue().reasons().stream().distinct().map(f ->
                "\n  Code: " + f.code() + "; Topic: " + entry.getKey()
                        + "; Partition: " + (f.partition().isPresent() ? f.partition().getAsInt() : "ALL")
                        + "; Broker: " + (f.brokerId().isPresent() ? f.brokerId().getAsInt() : "NOT_SCOPED")
                        + "; Actual: " + f.actual() + "; Required: " + f.required() + "; State: " + f.condition()))
                .limit(100).collect(Collectors.joining());
        return (codes.isEmpty() ? "RF execution not implemented. Preflight passed."
                : "RF preflight BLOCKED: " + codes + ".") + details
                + "\n  Action: resolve the reported invariant and re-plan. No topic operations were started; no Kafka resource writes were sent.";
    }
}
