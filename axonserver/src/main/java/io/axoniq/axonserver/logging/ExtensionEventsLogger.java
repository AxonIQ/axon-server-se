/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.logging;

import io.axoniq.axonserver.extensions.ExtensionEvent;
import io.axoniq.axonserver.interceptor.ExtensionEnabledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class ExtensionEventsLogger {

    private final Logger logger = LoggerFactory.getLogger(ExtensionEventsLogger.class);

    @EventListener
    public void on(ExtensionEnabledEvent extensionEnabled) {
        logger.info("{}: Extension {} updated for context, status = {}", extensionEnabled.context(),
                    extensionEnabled.extension(),
                    extensionEnabled.enabled() ? "Enabled" : "Disabled");
    }

    @EventListener
    public void on(ExtensionEvent statusChanged) {
        logger.info("Extension {} updated, status = {}",
                    statusChanged.getExtension(),
                    statusChanged.getStatus());
    }
}
