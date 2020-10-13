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

import java.io.InputStream;

/**
 * Container for all the defined interceptors for event and snapshot operations.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface EventInterceptors {

    /**
     * Intercepts an append event action. The implementation of the interceptor can update the event.
     *
     * @param interceptorContext the caller's context
     * @param eventInputStream   the new event
     * @return the new event
     */
    InputStream eventPreCommit(InterceptorContext interceptorContext, InputStream eventInputStream);

    /**
     * Intercepts an append snapshot action. The implementation of the interceptor can update the snapshot.
     *
     * @param interceptorContext the caller's context
     * @param snapshot           the new snapshot
     * @return the new event
     */
    Event snapshotPreRequest(InterceptorContext interceptorContext, Event snapshot);

    /**
     * Intercepts a snapshot read from the event store. The implementation of the interceptor can update the snapshot.
     *
     * @param interceptorContext the caller's context
     * @param snapshot           the read snapshot
     * @return the read snapshot
     */
    Event readSnapshot(InterceptorContext interceptorContext, Event snapshot);

    /**
     * Intercepts an event read from the event store. The implementation of the interceptor can update the event.
     *
     * @param interceptorContext the caller's context
     * @param event              the read event
     * @return the read event
     */
    Event readEvent(InterceptorContext interceptorContext, Event event);

    /**
     * Checks if there aren't any interceptors for reading events or snapshots.
     *
     * @return true if there are no interceptors for reading events or snapshots
     */
    boolean noReadInterceptors();

    /**
     * Checks if there aren't any interceptors for reading events.
     *
     * @return true if there are no interceptors for reading events
     */
    boolean noEventReadInterceptors();
}
