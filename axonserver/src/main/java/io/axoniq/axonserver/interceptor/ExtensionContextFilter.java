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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiPredicate;

/**
 * @author Marc Gathier
 */
@Component
public class ExtensionContextFilter implements BiPredicate<String, ExtensionKey> {

    private final Logger logger = LoggerFactory.getLogger(ExtensionContextFilter.class);
    private final Map<String, Set<ExtensionKey>> enabledExtensionsPerContext = new HashMap<>();

    @EventListener
    @Order(100)
    public void on(ExtensionEnabledEvent extensionEnabled) {
        if (extensionEnabled.enabled()) {
            if (enabledExtensionsPerContext.computeIfAbsent(extensionEnabled.context(),
                                                            c -> new CopyOnWriteArraySet<>())
                                           .add(extensionEnabled.extension())) {
                logger.warn("{}: Extension {} activated", extensionEnabled.context(), extensionEnabled.extension());
            }
        } else {
            if (enabledExtensionsPerContext.getOrDefault(extensionEnabled.context(), Collections.emptySet()).remove(
                    extensionEnabled.extension())) {
                logger.warn("{}: Extension {} deactivated", extensionEnabled.context(), extensionEnabled.extension());
            }
        }
    }

    @Override
    public boolean test(String context, ExtensionKey extensionKey) {
        return enabledExtensionsPerContext.getOrDefault(context, Collections.emptySet())
                                          .contains(extensionKey);
    }
}
