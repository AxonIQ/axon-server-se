/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.ExecutionContext;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * @author Marc Gathier
 */
public class TestExecutionContext implements ExecutionContext {

    private final String context;

    public TestExecutionContext(String context) {
        this.context = context;
    }

    @Override
    public String contextName() {
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
    public Map<String, String> principalTags() {
        return null;
    }

    @Override
    public void putAttribute(String key, Object value) {

    }

    @Override
    public Object getAttribute(String key) {
        return null;
    }

    @Override
    public void onFailure(BiConsumer<Throwable, ExecutionContext> compensatingAction) {

    }
}
