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
import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Controller;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Implements the {@link PluginController} for Axon Server Standard Edition.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Controller
public class DefaultPluginController implements PluginController {

    private final PluginPackageManager pluginPackageManager;
    private final PluginConfigurationManager configurationManager;
    private final PluginContextManager pluginContextManager;
    private final PluginConfigurationSerializer pluginConfigurationSerializer;

    public DefaultPluginController(PluginPackageManager pluginPackageManager,
                                   PluginConfigurationManager configurationManager,
                                   PluginContextManager pluginContextManager,
                                   PluginConfigurationSerializer pluginConfigurationSerializer) {
        this.pluginPackageManager = pluginPackageManager;
        this.configurationManager = configurationManager;
        this.pluginContextManager = pluginContextManager;
        this.pluginConfigurationSerializer = pluginConfigurationSerializer;
    }

    @Override
    public Iterable<PluginInfo> listPlugins() {
        return pluginPackageManager.listPlugins();
    }

    @Override
    public void uninstallPlugin(PluginKey pluginKey) {
        pluginPackageManager.uninstallPlugin(pluginKey);
    }

    @Override
    public PluginKey addPlugin(String fileName, InputStream inputStream) {
        return pluginPackageManager.addPlugin(fileName, inputStream).getKey();
    }

    @Override
    public List<PluginPropertyGroup> listProperties(PluginKey pluginKey, String context) {
        List<PluginPropertyGroup> definedProperties = configurationManager.configuration(pluginKey);
        pluginContextManager.getStatus(Topology.DEFAULT_CONTEXT,
                                       pluginKey.getSymbolicName(),
                                       pluginKey.getVersion())
                            .ifPresent(status -> PluginPropertyUtils.setValues(definedProperties,
                                                                               status
                                                                                       .getConfiguration(),
                                                                               pluginConfigurationSerializer));
        return definedProperties;
    }

    @Override
    public void updateConfiguration(PluginKey pluginKey, String context,
                                    Map<String, Map<String, Object>> properties) {
        properties = PluginPropertyUtils.validateProperties(properties,
                                                            listProperties(pluginKey, Topology.DEFAULT_CONTEXT));
        pluginContextManager.updateConfiguration(Topology.DEFAULT_CONTEXT,
                                                 pluginKey.getSymbolicName(),
                                                 pluginKey.getVersion(),
                                                 properties);
    }

    @Override
    public void updatePluginStatus(PluginKey pluginKey, String context, boolean active) {
        pluginContextManager.updateStatus(Topology.DEFAULT_CONTEXT,
                                          pluginKey.getSymbolicName(),
                                          pluginKey.getVersion(),
                                          active);
    }

    @Override
    public void unregisterPluginForContext(PluginKey pluginKey, String context) {
        pluginContextManager.removeForContext(pluginKey, Topology.DEFAULT_CONTEXT);
    }
}
