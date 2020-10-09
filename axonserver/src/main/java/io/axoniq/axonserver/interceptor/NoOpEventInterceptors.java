/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.event.Event;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import java.io.InputStream;

/**
 * @author Marc Gathier
 */
@Component
@ConditionalOnMissingBean(EventInterceptors.class)
public class NoOpEventInterceptors implements EventInterceptors {

    @Override
    public InputStream eventPreCommit(InterceptorContext interceptorContext, InputStream eventInputStream) {
        return eventInputStream;
    }

    @Override
    public Event snapshotPreRequest(InterceptorContext interceptorContext, Event event) {
        return event;
    }

    @Override
    public boolean noReadInterceptors() {
        return false;
    }

    @Override
    public Event readSnapshot(InterceptorContext interceptorContext, Event event) {
        return event;
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
