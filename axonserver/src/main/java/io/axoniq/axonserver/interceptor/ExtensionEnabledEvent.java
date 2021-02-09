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

import java.util.Map;

/**
 * @author Marc Gathier
 */
public class ExtensionEnabledEvent {

    private final String context;
    private final ExtensionKey extension;
    private final Map<String, Map<String, Object>> configuration;
    private final boolean enabled;

    public ExtensionEnabledEvent(String context, ExtensionKey extension, Map<String, Map<String, Object>> configuration,
                                 boolean enabled) {
        this.context = context;
        this.extension = extension;
        this.configuration = configuration;
        this.enabled = enabled;
    }

    public String context() {
        return context;
    }

    public ExtensionKey extension() {
        return extension;
    }

    public boolean enabled() {
        return enabled;
    }

    public Map<String, Map<String, Object>> configuration() {
        return configuration;
    }
}
