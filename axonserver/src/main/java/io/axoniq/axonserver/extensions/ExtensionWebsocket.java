/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.interceptor.ExtensionEnabledEvent;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * @author Marc Gathier
 */
@Service
public class ExtensionWebsocket {

    private final SimpMessagingTemplate websocket;

    public ExtensionWebsocket(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(ExtensionEnabledEvent event) {
        websocket.convertAndSend("/topic/extensions", new ExtensionEvent(event.extension()));
    }

    @EventListener
    public void on(ExtensionEvent event) {
        websocket.convertAndSend("/topic/extensions", event);
    }
}
