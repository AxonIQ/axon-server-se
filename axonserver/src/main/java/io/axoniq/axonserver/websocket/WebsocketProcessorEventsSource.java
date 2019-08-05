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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.EmitterProcessor;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sends {@link EventProcessorStatusUpdate} events to websockets
 * @author Sara Pellegrini
 * @since 4.0
 */
@Service
public class WebsocketProcessorEventsSource {

    private final EmitterProcessor<EventProcessorStatusUpdate> updateFlux;

    @Autowired
    public WebsocketProcessorEventsSource(SimpMessagingTemplate websocket) {
        this(updates -> websocket.convertAndSend("/topic/processor", EventProcessorStatusUpdate.class.getName()), 500);
    }

    public WebsocketProcessorEventsSource(Consumer<List<EventProcessorStatusUpdate>> updatesConsumer,
                                          long milliseconds) {
        this.updateFlux = EmitterProcessor.create(100);
        updateFlux.buffer(Duration.ofMillis(milliseconds)).subscribe(updatesConsumer);
    }

    @EventListener
    public void on(EventProcessorStatusUpdate event) {
        updateFlux.onNext(event);
    }
}
