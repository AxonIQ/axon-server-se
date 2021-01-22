/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

/**
 * Checks if an extension is active for a specific context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionContextFilter implements BiPredicate<String, ExtensionKey> {

    private final Logger logger = LoggerFactory.getLogger(ExtensionContextFilter.class);
    private final Map<String, Map<String, String>> enabledExtensionsPerContext = new ConcurrentHashMap<>();

    /**
     * Handles {@link ExtensionEnabledEvent} events, published when an extension becomes active or is
     * de-activated for a specific context.
     * @param extensionEnabled the event
     */
    @EventListener
    @Order(100)
    public void on(ExtensionEnabledEvent extensionEnabled) {
        if (extensionEnabled.enabled()) {
            String oldVersion = enabledExtensionsPerContext.computeIfAbsent(extensionEnabled.context(),
                                                                            c -> new ConcurrentHashMap<>())
                                                           .put(extensionEnabled.extension().getSymbolicName(),
                                                                extensionEnabled.extension().getVersion());
            if (oldVersion == null || !extensionEnabled.extension().getVersion().equals(oldVersion)) {
                logger.info("{}: Extension {} activated", extensionEnabled.context(), extensionEnabled.extension());
            }
        } else {
            String oldVersion = enabledExtensionsPerContext.getOrDefault(extensionEnabled.context(),
                                                                         Collections.emptyMap())
                                                           .get(extensionEnabled.extension().getSymbolicName());
            if (oldVersion != null && extensionEnabled.extension().getVersion().equals(oldVersion)) {
                enabledExtensionsPerContext.get(extensionEnabled.context())
                                           .remove(extensionEnabled.extension().getSymbolicName());
                logger.info("{}: Extension {} deactivated", extensionEnabled.context(), extensionEnabled.extension());
            }
        }
    }

    /**
     * Checks if an extension is active for a specific context.
     *
     * @param context      the name of the context
     * @param extensionKey the key of the extension
     * @return true if the extension is active for the given context
     */
    @Override
    public boolean test(String context, ExtensionKey extensionKey) {
        return extensionKey.getVersion().equals(
                enabledExtensionsPerContext.getOrDefault(context, Collections.emptyMap())
                                           .get(extensionKey.getSymbolicName()));
    }
}
