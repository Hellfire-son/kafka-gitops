// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.domain.plan;

import tools.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(builder = PlanOverview.Builder.class)
@JsonDeserialize(builder = PlanOverview.Builder.class)
public interface PlanOverview {

    Long getAdd();

    Long getRemove();

    Long getUpdate();

    Long getNoChange();

    class Builder extends PlanOverview_Builder {
    }
}
