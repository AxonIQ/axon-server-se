/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.logging;

import io.axoniq.axonserver.interceptor.PluginRemovedEvent;
import io.axoniq.axonserver.plugin.PluginEvent;
import io.axoniq.axonserver.interceptor.PluginEnabledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class PluginEventsLogger {

    private final Logger logger = LoggerFactory.getLogger(PluginEventsLogger.class);

    @EventListener
    public void on(PluginEnabledEvent pluginEnabledEvent) {
        logger.info("{}: Plugin {} updated for context, status = {}", pluginEnabledEvent.context(),
                    pluginEnabledEvent.plugin(),
                    pluginEnabledEvent.enabled() ? "Enabled" : "Disabled");
    }

    @EventListener
    public void on(PluginRemovedEvent pluginRemovedEvent) {
        logger.info("{}: Plugin {} removed for context", pluginRemovedEvent.context(),
                    pluginRemovedEvent.plugin());
    }

    @EventListener
    public void on(PluginEvent statusChanged) {
        logger.info("Plugin {} updated, status = {}",
                    statusChanged.getPlugin(),
                    statusChanged.getStatus());
    }
}
