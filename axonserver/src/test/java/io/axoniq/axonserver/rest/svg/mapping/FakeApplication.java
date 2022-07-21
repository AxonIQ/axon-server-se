/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

import java.util.Arrays;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeApplication implements Application {

    private final String name;
    private final String component;
    private final String context;
    private final int instances;
    private final Iterable<String> connectedhubNodes;

    public FakeApplication(String name, String component, String context, int instances,
                           Iterable<String> connectedhubNodes) {
        this.name = name;
        this.component = component;
        this.context = context;
        this.instances = instances;
        this.connectedhubNodes = connectedhubNodes;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public String component() {
        return component;
    }

    @Override
    public Iterable<String> contexts() {
        return Arrays.asList(context);
    }

    @Override
    public int instances() {
        return instances;
    }

    @Override
    public Iterable<String> connectedHubNodes() {
        return connectedhubNodes;
    }
}
