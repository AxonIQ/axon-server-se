/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.PluginKey;

import java.util.Map;

/**
 * @author Marc Gathier
 */
public class PluginEnabledEvent {

    private final String context;
    private final PluginKey plugin;
    private final Map<String, Map<String, Object>> configuration;
    private final boolean enabled;

    public PluginEnabledEvent(String context, PluginKey plugin, Map<String, Map<String, Object>> configuration,
                              boolean enabled) {
        this.context = context;
        this.plugin = plugin;
        this.configuration = configuration;
        this.enabled = enabled;
    }

    public String context() {
        return context;
    }

    public PluginKey plugin() {
        return plugin;
    }

    public boolean enabled() {
        return enabled;
    }

    public Map<String, Map<String, Object>> configuration() {
        return configuration;
    }
}
