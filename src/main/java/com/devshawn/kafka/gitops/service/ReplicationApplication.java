// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.plan.TopicPlan;

import java.util.List;

/** Internal integration seam. No CLI/JSON option may install an implementation or supply safety evidence. */
@FunctionalInterface
public interface ReplicationApplication {
    ReassignmentExecutor.ExecutionResult apply(List<TopicPlan> topics, ReplicationSafetyPolicy.Permissions permissions);
}
