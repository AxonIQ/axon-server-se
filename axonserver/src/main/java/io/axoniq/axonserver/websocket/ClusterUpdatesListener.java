/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Spring event listener that publishes events to connected dashboards via the websocket.
 *
 * @author Marc Gathier
 */
@Service
public class ClusterUpdatesListener {
    private final SimpMessagingTemplate websocket;

    public ClusterUpdatesListener(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(TopologyEvents.TopologyBaseEvent clusterEvent) {
        websocket.convertAndSend("/topic/cluster", clusterEvent.getClass().getName());
    }

    @EventListener
    public void on(SubscriptionEvents.SubscriptionBaseEvent subscriptionEvent) {
        websocket.convertAndSend("/topic/subscriptions", subscriptionEvent.getClass().getName());
    }
}
