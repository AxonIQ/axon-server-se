/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.interceptor.ExtensionEnabledEvent;
import io.axoniq.axonserver.rest.ExtensionPropertyGroup;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages the configuration of extensions.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class ExtensionConfigurationManager {

    private final Logger logger = LoggerFactory.getLogger(ExtensionConfigurationManager.class);

    private final OsgiController osgiController;

    public ExtensionConfigurationManager(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    @EventListener
    @Order(0)
    public void on(ExtensionEnabledEvent extensionEnabledEvent) {
        if (extensionEnabledEvent.enabled()) {
            updateConfiguration(extensionEnabledEvent.extension(),
                                extensionEnabledEvent.context(),
                                extensionEnabledEvent.configuration());
        }
    }

    /**
     * Updates the configuration of an extension.
     *
     * @param bundleInfo the name and version of the extension
     * @param properties the new properties
     */
    public void updateConfiguration(ExtensionKey bundleInfo, String context,
                                    Map<String, Map<String, Object>> properties) {
        Set<ConfigurationListener> configurationListeners = osgiController.getConfigurationListeners(bundleInfo);

        Map<String, Map<String, Object>> nonNullProperties = properties == null ? Collections.emptyMap() : properties;
        configurationListeners.forEach(listener -> listener.updated(context, nonNullProperties.get(listener.id())));
    }


    /**
     * @param bundleInfo name and version of the bundle
     * @return list of properties that can be set for the extension
     */
    public List<ExtensionPropertyGroup> configuration(ExtensionKey bundleInfo) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        if (bundle == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found");
        }

        List<ExtensionPropertyGroup> extensionProperties = new ArrayList<>();
        osgiController.getConfigurationListeners(bundleInfo)
                      .forEach(configurationListener ->
                                       extensionProperties.add(new ExtensionPropertyGroup(configurationListener.id(),
                                                                                          configurationListener.id(),
                                                                                          configurationListener
                                                                                                  .attributes()
                                                                                                  .stream()
                                                                                                  .map(ExtensionProperty::new)
                                                                                                  .collect(Collectors
                                                                                                                   .toList()))));

        return extensionProperties;
    }
}
