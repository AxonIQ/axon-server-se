/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.instance;

/**
 * Created by Sara Pellegrini on 30/03/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeClient implements Client {

    private final String name;

    private final String context;

    private final boolean belongs;

    public FakeClient(String name, boolean belongs) {
        this(name, "no context", belongs);
    }

    public FakeClient(String name, String context, boolean belongs) {
        this.name = name;
        this.context = context;
        this.belongs = belongs;
    }

    @Override
    public String id() {
        return name;
    }

    @Override
    public String streamId() {
        return name;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return belongs;
    }
}
