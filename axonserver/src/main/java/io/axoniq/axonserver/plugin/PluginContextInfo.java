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
 * Value object containing the status of a plugin per context.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class PluginContextInfo {

    private final String context;
    private final boolean active;

    public PluginContextInfo(String context, boolean active) {
        this.context = context;
        this.active = active;
    }

    public String getContext() {
        return context;
    }

    public boolean isActive() {
        return active;
    }
}
