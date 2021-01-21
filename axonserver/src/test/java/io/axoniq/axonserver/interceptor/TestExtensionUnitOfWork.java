/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class TestExtensionUnitOfWork implements ExtensionUnitOfWork {

    private final String context;

    public TestExtensionUnitOfWork(String context) {
        this.context = context;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public String principal() {
        return null;
    }

    @Override
    public Set<String> principalRoles() {
        return null;
    }

    @Override
    public Map<String, String> principalMetaData() {
        return null;
    }

    @Override
    public void addDetails(String key, Object value) {

    }

    @Override
    public Object getDetails(String key) {
        return null;
    }

    @Override
    public void onFailure(Consumer<ExtensionUnitOfWork> compensatingAction) {

    }
}
