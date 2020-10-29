/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import io.axoniq.axonserver.extensions.interceptor.InterceptorContext;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.interceptor.EventInterceptors;

import java.io.InputStream;

/**
 * @author Marc Gathier
 */
public class NoOpEventInterceptors implements EventInterceptors {

    @Override
    public InputStream appendEvent(InterceptorContext interceptorContext, InputStream eventInputStream) {
        return eventInputStream;
    }

    @Override
    public void eventsPostCommit(InterceptorContext interceptorContext) {
    }

    @Override
    public Event snapshotPreRequest(InterceptorContext interceptorContext, Event event) {
        return event;
    }

    @Override
    public void eventsPreCommit(InterceptorContext interceptorContext) {
    }

    @Override
    public boolean noReadInterceptors() {
        return false;
    }

    @Override
    public Event readSnapshot(InterceptorContext interceptorContext, Event snapshot) {
        return snapshot;
    }

    @Override
    public Event readEvent(InterceptorContext interceptorContext, Event event) {
        return event;
    }

    @Override
    public boolean noEventReadInterceptors() {
        return false;
    }
}
