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
 * Provides the context for intercepted requests. It contains information on the caller and the Axon Server context
 * of the request. The same context object is used for the whole interceptor chain for a request.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class InterceptorContext {

    private final String context;
    private final Authentication principal;
    private final List<Runnable> compensatingActions = new LinkedList<>();

    /**
     * @param context   the Axon Server context for the request
     * @param principal the caller's information
     */
    public InterceptorContext(String context, Authentication principal) {
        this.context = context;
        this.principal = principal;
    }

    /**
     * Returns the Axon Server context for the request.
     *
     * @return the Axon Server context
     */
    public String context() {
        return context;
    }

    /**
     * Returns the caller's information.
     *
     * @return the caller's information
     */
    public Authentication principal() {
        return principal;
    }

    /**
     * Registers an action to call when request execution fails, or any of the subsequent interceptors fail.
     *
     * @param compensatingAction runnable compensation action
     */
    public void registerCompensatingAction(Runnable compensatingAction) {
        compensatingActions.add(0, compensatingAction);
    }

    /**
     * Execute all compensating actions. The last registered compensating action is executed first.
     */
    public void compensate() {
        compensatingActions.forEach(Runnable::run);
    }
}
