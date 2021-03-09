/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.plugin;

/**
 * Event published when a plugin is installed/removed.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginEvent {

    private final PluginKey plugin;
    private final String status;

    public PluginEvent(PluginKey plugin) {
        this(plugin, null);
    }

    public PluginEvent(PluginKey extensionKey, String status) {
        this.plugin = extensionKey;
        this.status = status;
    }

    public PluginKey getPlugin() {
        return plugin;
    }

    public String getStatus() {
        return status;
    }
}
