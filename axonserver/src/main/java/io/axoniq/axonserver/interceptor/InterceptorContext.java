/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import org.springframework.security.core.Authentication;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class InterceptorContext {

    private final String context;
    private final Authentication principal;
    private final List<Runnable> compensatingActions = new LinkedList<>();

    public InterceptorContext(String context, Authentication principal) {
        this.context = context;
        this.principal = principal;
    }

    public void compensate() {
        compensatingActions.forEach(Runnable::run);
    }

    public String context() {
        return context;
    }

    public Authentication principal() {
        return principal;
    }

    public void addCompensatingAction(Runnable compensatingAction) {
        compensatingActions.add(0, compensatingAction);
    }
}
