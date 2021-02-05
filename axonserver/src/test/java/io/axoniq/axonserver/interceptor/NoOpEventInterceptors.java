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
import io.axoniq.axonserver.grpc.event.Event;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class NoOpEventInterceptors implements EventInterceptors {

    @Override
    public Event appendEvent(Event event, ExtensionUnitOfWork interceptorContext) {
        return event;
    }

    @Override
    public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        return snapshot;
    }

    @Override
    public void eventsPreCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {

    }

    @Override
    public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {

    }

    @Override
    public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork interceptorContext) {

    }

    @Override
    public Event readSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
        return snapshot;
    }

    @Override
    public Event readEvent(Event event, ExtensionUnitOfWork interceptorContext) {
        return event;
    }

    @Override
    public boolean noReadInterceptors(String context) {
        return true;
    }

    @Override
    public boolean noEventReadInterceptors(String context) {
        return true;
    }
}
