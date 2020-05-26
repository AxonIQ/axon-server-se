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

import java.util.Map;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class GenericClient implements Client {

    private final String clientId;

    private final String componentName;

    private final String context;
    private final String axonServerNode;
    private final Map<String, String> tags;

    public GenericClient(String clientId, String componentName, String context, String axonServerNode,
                         Map<String, String> tags) {
        this.clientId = clientId;
        this.componentName = componentName;
        this.context = context;
        this.axonServerNode = axonServerNode;
        this.tags = tags;
    }

    @Override
    public String name() {
        return clientId;
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
        media.with("name", name())
             .with("componentName", componentName)
             .with("axonServerNode", axonServerNode)
             .with("tags", tags);
    }
}
