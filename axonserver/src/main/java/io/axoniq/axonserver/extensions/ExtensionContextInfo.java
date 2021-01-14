/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions;

/**
 * @author Marc Gathier
 */
public class ExtensionContextInfo {

    private final String context;
    private final String configuration;
    private final boolean active;

    public ExtensionContextInfo(String context, String configuration, boolean active) {
        this.context = context;
        this.configuration = configuration;
        this.active = active;
    }

    public String getContext() {
        return context;
    }

    public String getConfiguration() {
        return configuration;
    }

    public boolean isActive() {
        return active;
    }
}
