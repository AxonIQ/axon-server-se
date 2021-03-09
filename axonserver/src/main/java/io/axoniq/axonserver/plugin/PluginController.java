/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

import io.axoniq.axonserver.rest.PluginPropertyGroup;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Manages the plugins.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface PluginController {

    /**
     * @return iterator of the currently installed plugins
     */
    Iterable<PluginInfo> listPlugins();

    /**
     * Uninstalls a plugin from Axon Server.
     *
     * @param extensionKey the name and version of the plugins
     */
    void uninstallPlugin(PluginKey extensionKey);

    /**
     * Adds or updates a plugin. If there is already a plugin with the same name and the same version it is
     * replaced.
     *
     * @param fileName    the name of the plugin file
     * @param inputStream input stream for the jar file for the plugin
     * @return name and version of the plugin
     */
    PluginKey addPlugin(String fileName, InputStream inputStream);

    /**
     * Lists the properties and their current values for a plugin within a specific context. The result is a
     * list of properties per group, containing the description and value for each property.
     *
     * @param pluginKey the name and version of the plugin
     * @param context   the name of the context
     * @return the properties
     */
    List<PluginPropertyGroup> listProperties(PluginKey pluginKey, String context);

    /**
     * Sets the properties for a plugin for a context.
     *
     * @param pluginKey  the name and version of the plugin
     * @param context    the name of the context
     * @param properties the new property values
     */
    void updateConfiguration(PluginKey pluginKey, String context, Map<String, Map<String, Object>> properties);

    /**
     * Activates/pauses the plugin for a specific context.
     *
     * @param pluginKey the name and version of the plugin
     * @param context   the name of the context
     * @param active    {@code true} to activate the property
     */
    void updatePluginStatus(PluginKey pluginKey, String context, boolean active);

    /**
     * Unregisters a plugin for a specific context. Configuration for the plugin for the context is removed.
     *
     * @param pluginKey the name and version of the plugin
     * @param context   the name of the context
     */
    void unregisterPluginForContext(PluginKey pluginKey, String context);
}
