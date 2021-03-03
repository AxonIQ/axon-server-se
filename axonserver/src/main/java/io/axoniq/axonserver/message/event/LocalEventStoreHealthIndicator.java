/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.boot.actuate.autoconfigure.system.DiskSpaceHealthIndicatorProperties;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
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
    protected void doHealthCheck(Health.Builder builder)  {
        builder.up();
        clusterController.getMyContextNames().forEach(context -> {
            try {
                builder.withDetail(String.format("%s.lastEvent", context), localEventStore.getLastEvent(context));
                builder.withDetail(String.format("%s.lastSnapshot", context), localEventStore.getLastSnapshot(context));
                builder.withDetail(String.format("%s.waitingEventTransactions", context),
                                   localEventStore.getWaitingEventTransactions(context));
                builder.withDetail(String.format("%s.waitingSnapshotTransactions", context),
                                   localEventStore.getWaitingSnapshotTransactions(context));
            } catch (MessagingPlatformException mpe) {
                // ignore
            }
        });
    }
}
