/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.extensions.ExtensionProperty;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
public class ExtensionPropertyGroup {

    private final String id;
    private final String name;
    private final Collection<ExtensionProperty> properties;

    public ExtensionPropertyGroup(String id, String name,
                                  Collection<ExtensionProperty> properties) {
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

    public Collection<ExtensionProperty> getProperties() {
        return properties;
    }
}
