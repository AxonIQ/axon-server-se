/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.interceptor.PluginEnabledEvent;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Websocket to notify the UI of plugin updates.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Service
public class PluginWebsocket {

    private final SimpMessagingTemplate websocket;

    public PluginWebsocket(SimpMessagingTemplate websocket) {
        this.websocket = websocket;
    }

    @EventListener
    public void on(PluginEnabledEvent event) {
        websocket.convertAndSend("/topic/extensions", new PluginEvent(event.plugin()));
    }

    @EventListener
    public void on(PluginEvent event) {
        websocket.convertAndSend("/topic/extensions", event);
    }
}
