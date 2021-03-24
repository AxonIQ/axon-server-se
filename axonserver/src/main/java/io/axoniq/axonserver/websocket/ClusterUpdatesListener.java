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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Spring event listener that publishes events to connected dashboards via the websocket.
 *
 * @author Marc Gathier
 * @author Stefan Dragisic
 */
@Service
public class ClusterUpdatesListener {

    private final Sinks.Many<TopologyEvents.TopologyBaseEvent> topologyBaseEventEmitterSink = Sinks.many().multicast().onBackpressureBuffer(100, false);
    private final Sinks.Many<SubscriptionEvents.SubscriptionBaseEvent> subscriptionBaseEventEmitterSink = Sinks.many().multicast().onBackpressureBuffer(100, false);

    @Autowired
    public ClusterUpdatesListener(SimpMessagingTemplate websocket) {
        this(topologyBaseEvents -> websocket
                        .convertAndSend("/topic/cluster", topologyBaseEvents.get(0).getClass().getName()),
                subscriptionEvents -> websocket
                        .convertAndSend("/topic/subscriptions", subscriptionEvents.get(0).getClass().getName()),
                1000
        );
    }

    public ClusterUpdatesListener(
            Consumer<List<TopologyEvents.TopologyBaseEvent>> topologyUpdatesConsumer,
            Consumer<List<SubscriptionEvents.SubscriptionBaseEvent>> subscriptionUpdatesConsumer,
            long milliseconds) {
        topologyBaseEventEmitterSink.asFlux().publishOn(Schedulers.boundedElastic()).buffer(Duration.ofMillis(milliseconds)).subscribe(topologyUpdatesConsumer);
        subscriptionBaseEventEmitterSink.asFlux().publishOn(Schedulers.boundedElastic()).buffer(Duration.ofMillis(milliseconds)).subscribe(subscriptionUpdatesConsumer);
    }

    @EventListener
    public void on(TopologyEvents.TopologyBaseEvent clusterEvent) {
        while (topologyBaseEventEmitterSink.tryEmitNext(clusterEvent) == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            //busy wait
        }
    }

    @EventListener
    public void on(SubscriptionEvents.SubscriptionBaseEvent subscriptionEvent) {
        while (subscriptionBaseEventEmitterSink.tryEmitNext(subscriptionEvent) == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            //busy wait
        }
    }
}
