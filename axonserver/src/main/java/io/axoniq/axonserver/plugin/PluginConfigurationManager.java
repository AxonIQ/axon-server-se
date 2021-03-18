/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.interceptor.PluginEnabledEvent;
import io.axoniq.axonserver.interceptor.PluginRemovedEvent;
import io.axoniq.axonserver.rest.PluginPropertyGroup;
import org.osgi.framework.Bundle;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Facade to update configuration on installed plugins and to
 * retrieve the defined configuration items for a plugin.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class PluginConfigurationManager {

    private final OsgiController osgiController;

    public PluginConfigurationManager(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    /**
     * Handles an {@link PluginEnabledEvent}. If the event indicates that a plugin is now available,
     * it forwards the configuration properties to the plugin.
     *
     * @param pluginEnabledEvent the event
     */
    @EventListener
    @Order(0)
    public void on(PluginEnabledEvent pluginEnabledEvent) {
        if (pluginEnabledEvent.enabled()) {
            updateConfiguration(pluginEnabledEvent.plugin(),
                                pluginEnabledEvent.context(),
                                pluginEnabledEvent.configuration());
        }
    }

    @EventListener
    @Order(0)
    public void on(PluginRemovedEvent removedEvent) {
        Set<ConfigurationListener> configurationListeners = osgiController.getConfigurationListeners(removedEvent
                                                                                                             .plugin());
        configurationListeners.forEach(listener -> listener.removed(removedEvent.context()));
    }

    /**
     * Updates the configuration for a context in a plugin.
     *
     * @param bundleInfo the name and version of the plugin
     * @param context    the context for the configuration
     * @param properties the new properties
     */
    public void updateConfiguration(PluginKey bundleInfo, String context,
                                    Map<String, Map<String, Object>> properties) {
        Set<ConfigurationListener> configurationListeners = osgiController.getConfigurationListeners(bundleInfo);

        Map<String, Map<String, Object>> nonNullProperties =
                properties == null ? defaultProperties(bundleInfo) : properties;
        configurationListeners.forEach(listener -> listener
                .updated(context, nonNullProperties.get(listener.configuration().name())));
    }

    private Map<String, Map<String, Object>> defaultProperties(PluginKey bundleInfo) {
        List<PluginPropertyGroup> groups = configuration(bundleInfo);
        Map<String, Map<String, Object>> defaultProperties = new HashMap<>();
        groups.forEach(group -> {
            Map<String, Object> groupProperties = new HashMap<>();
            group.getProperties().forEach(property -> {
                if (property.getDefaultValue() != null) {
                    groupProperties.put(property.getId(), property.getDefaultValue());
                }
            });
            defaultProperties.put(group.getId(), groupProperties);
        });
        return defaultProperties;
    }


    /**
     * Retrieves the defined configuration items for a plugin. A plugin can define multiple
     * configuration groups, each containing their own properties.
     *
     * @param bundleInfo name and version of the bundle
     * @return list of properties that can be set for the plugin
     */
    public List<PluginPropertyGroup> configuration(PluginKey bundleInfo) {
        Bundle bundle = osgiController.getBundle(bundleInfo);
        if (bundle == null) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Bundle not found");
        }

        List<PluginPropertyGroup> pluginPropertyGroups = new ArrayList<>();
        osgiController.getConfigurationListeners(bundleInfo)
                      .forEach(configurationListener -> {
                          Configuration configuration = configurationListener.configuration();
                          pluginPropertyGroups
                                  .add(new PluginPropertyGroup(configuration.name(),
                                                               configuration.name(),
                                                               configuration.properties()
                                                                            .stream()
                                                                            .map(PluginProperty::new)
                                                                            .collect(Collectors
                                                                                             .toList())));
                      });

        return pluginPropertyGroups;
    }
}
