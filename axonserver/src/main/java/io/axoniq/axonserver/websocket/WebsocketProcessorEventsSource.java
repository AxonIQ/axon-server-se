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
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Sends {@link EventProcessorStatusUpdate} events to websockets
 * @author Sara Pellegrini
 * @since 4.0
 */
@Service
public class WebsocketProcessorEventsSource {

    private final SimpMessagingTemplate websocket;

    public WebsocketProcessorEventsSource(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(EventProcessorStatusUpdate event) {
        websocket.convertAndSend("/topic/processor", event.getClass().getName());
    }
}
