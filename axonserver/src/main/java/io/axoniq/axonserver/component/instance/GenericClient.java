/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

import io.axoniq.axonserver.serializer.Media;

/**
 * Default implementation of a {@link Client}.
 */
public class GenericClient implements Client {

    private final String clientName;
    private final String componentName;

    private final String context;
    private final String axonServerNode;

    public GenericClient(String clientName,
                         String componentName,
                         String context,
                         String axonServerNode) {
        this.clientName = clientName;
        this.componentName = componentName;
        this.context = context;
        this.axonServerNode = axonServerNode;
    }

    @Override
    public String name() {
        return clientName;
    }


    @Override
    public String context() {
        return context;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return component.equals(componentName);
    }

    @Override
    public void printOn(Media media) {
        media.with("name", name()).with("componentName", componentName).with("axonServerNode", axonServerNode);
    }
}
