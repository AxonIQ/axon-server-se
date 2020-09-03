/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.websocket;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends {@link EventProcessorStatusUpdate} events to websockets. It only sends one event per scheduled interval
 * to the websocket to prevent flooding the consoles (and as the console calls the server on an event to retrieve the
 * full
 * status, it also limits load on the server).
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Service
public class WebsocketProcessorEventsSource {

    private final Runnable updatesConsumer;
    private final Logger logger = LoggerFactory.getLogger(WebsocketProcessorEventsSource.class);

    private final AtomicBoolean updates = new AtomicBoolean();

    /**
     * Autowired constructor for the service
     *
     * @param websocket the websocket holder to send the events to
     */
    @Autowired
    public WebsocketProcessorEventsSource(SimpMessagingTemplate websocket) {
        this(() -> websocket.convertAndSend("/topic/processor", EventProcessorStatusUpdate.class.getName()));
    }

    /**
     * Constuctor for testing purposes.
     *
     * @param updatesConsumer action to call when event has occurred
     */
    public WebsocketProcessorEventsSource(Runnable updatesConsumer) {
        this.updatesConsumer = updatesConsumer;
    }

    /**
     * Checks if there were new events since the last run and forwards the events
     */
    @Scheduled(initialDelayString = "${axoniq.axonserver.websocket-update.initial-delay:10000}",
            fixedRateString = "${axoniq.axonserver.websocket-update.rate:1000}")
    public void applyIfUpdates() {
        if (updates.compareAndSet(true, false)) {
            try {
                updatesConsumer.run();
            } catch (Exception ex) {
                // Ignore
                logger.warn("Sending to websocket failed", ex);
            }
        }
    }

    /**
     * Notifies the server that a tracking event processor has updates
     *
     * @param event the updated tracking event processor
     */
    @EventListener
    public void on(EventProcessorStatusUpdate event) {
        updates.set(true);
    }
}
