package com.devshawn.kafka.gitops.exception;

import com.devshawn.kafka.gitops.domain.plan.PlanningReport;

public final class PlanBlockedException extends ValidationException {
    private final PlanningReport report;
    public PlanBlockedException(PlanningReport report) {
        super("PLAN_BLOCKED: unresolved planning conditions; no changes were applied and no executable JSON was written.");
        this.report = report;
    }
    public PlanningReport getReport() { return report; }
}
