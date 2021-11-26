/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.logging;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Application events for application connects/disconnects.
 * @author Marc Gathier
 */
@Component
public class TopologyEventsLogger {
    private final Logger logger = LoggerFactory.getLogger(TopologyEventsLogger.class);

    @EventListener
    public void on(TopologyEvents.ApplicationConnected event) {
        if( event.isProxied()) {
            logger.info("Application connected via {}: {}, clientId = {}, clientStreamId = {}, context = {}",
                        event.getProxy(),
                        event.getComponentName(),
                        event.getClientId(),
                        event.getClientStreamId(),
                        event.getContext());
        } else {
            logger.info("Application connected: {}, clientId = {}, clientStreamId = {}, context = {}",
                        event.getComponentName(),
                        event.getClientId(),
                        event.getClientStreamId(),
                        event.getContext());
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationDisconnected event) {
        if( event.isProxied()) {
            logger.info("Application disconnected via {}: {}, clientId = {}, context = {}: {}",
                        event.getProxy(),
                        event.getComponentName(),
                        event.getClientStreamId(),
                        event.getContext(),
                        event.getReason());
        } else {
            logger.info("Application disconnected: {}, clientId = {}, context = {}: {}",
                        event.getComponentName(),
                        event.getClientStreamId(),
                        event.getContext(),
                        event.getReason());
        }
    }

    @EventListener
    public void on(TopologyEvents.ApplicationInactivityTimeout event) {
        logger.info("Application inactivity timeout for {} clientId = {}, context = {}",
                    event.componentName(),
                    event.client().clientId(),
                    event.client().context());

    }

    @EventListener
    public void on(TopologyEvents.ApplicationReconnectRequested event) {
        logger.info("Requested application reconnect for {} clientId = {}, context = {} - {}",
                    event.componentName(),
                    event.clientId(),
                    event.context(),
                    event.reason());
    }
}
