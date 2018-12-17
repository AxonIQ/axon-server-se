package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * Author: marc
 */
@Component
public class LocalEventStoreHealthIndicator extends AbstractHealthIndicator {
    private final LocalEventStore localEventStore;
    private final Topology clusterController;

    public LocalEventStoreHealthIndicator(LocalEventStore localEventStore,
                                          Topology clusterController) {
        this.localEventStore = localEventStore;
        this.clusterController = clusterController;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        clusterController.getMyContextNames().forEach(context -> {
            builder.withDetail(String.format("%s.lastEvent", context), localEventStore.getLastToken(context));
            builder.withDetail(String.format("%s.lastSnapshot", context), localEventStore.getLastSnapshot(context));
            builder.withDetail(String.format("%s.waitingEventTransactions", context), localEventStore.getWaitingEventTransactions(context));
            builder.withDetail(String.format("%s.waitingSnapshotTransactions", context), localEventStore.getWaitingSnapshotTransactions(context));
            localEventStore.health(builder);
        });
    }
}
