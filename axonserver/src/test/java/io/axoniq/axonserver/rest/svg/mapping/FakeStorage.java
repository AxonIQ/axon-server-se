/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeStorage implements Storage {

    private final String name;
    private final Iterable<String> contexts;
    private final Iterable<String> connectedHubNodes;

    public FakeStorage(String name, Iterable<String> contexts,
                       Iterable<String> connectedHubNodes) {
        this.name = name;
        this.contexts = contexts;
        this.connectedHubNodes = connectedHubNodes;
    }


    @Override
    public String context() {
        return contexts.iterator().next();
    }



    @Override
    public boolean master() {
        return false;
    }
}
