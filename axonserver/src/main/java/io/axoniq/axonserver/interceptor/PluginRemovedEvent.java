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
public class PluginRemovedEvent {

    private final String context;
    private final PluginKey plugin;

    public PluginRemovedEvent(String context, PluginKey plugin) {
        this.context = context;
        this.plugin = plugin;
    }

    public String context() {
        return context;
    }

    public PluginKey plugin() {
        return plugin;
    }
}
