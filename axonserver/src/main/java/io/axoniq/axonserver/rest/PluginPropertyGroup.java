/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.plugin.PluginProperty;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
public class PluginPropertyGroup {

    private final String id;
    private final String name;
    private final Collection<PluginProperty> properties;

    public PluginPropertyGroup(String id, String name,
                               Collection<PluginProperty> properties) {
        this.id = id;
        this.name = name;
        this.properties = properties;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Collection<PluginProperty> getProperties() {
        return properties;
    }
}
