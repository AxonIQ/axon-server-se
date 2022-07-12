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
import io.axoniq.axonserver.plugin.ExecutionContext;

import java.util.List;

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
     * @param event            the new event
     * @param executionContext the caller's context
     * @return the new event
     */
    Event interceptEvent(Event event, ExecutionContext executionContext);

    /**
     * Intercepts an append snapshot action. The implementation of the interceptor can update the snapshot.
     *
     * @param snapshot         the new snapshot
     * @param executionContext the caller's context
     * @return the new event
     */
    Event interceptSnapshot(Event snapshot, ExecutionContext executionContext);

    void interceptEventsPreCommit(List<Event> events, ExecutionContext executionContext);

    void interceptEventsPostCommit(List<Event> events, ExecutionContext executionContext);

    void interceptSnapshotPostCommit(Event snapshot, ExecutionContext executionContext);

    /**
     * Intercepts a snapshot read from the event store. The implementation of the interceptor can update the snapshot.
     *
     * @param snapshot         the read snapshot
     * @param executionContext the caller's context
     * @return the read snapshot
     */
    Event readSnapshot(Event snapshot, ExecutionContext executionContext);

    /**
     * Intercepts an event read from the event store. The implementation of the interceptor can update the event.
     *
     * @param event            the read event
     * @param executionContext the caller's context
     * @return the read event
     */
    Event readEvent(Event event, ExecutionContext executionContext);

    /**
     * Checks if there aren't any interceptors for reading events or snapshots.
     *
     * @return true if there are no interceptors for reading events or snapshots
     */
    boolean noReadInterceptors(String context);

    /**
     * Checks if there aren't any interceptors for reading events.
     *
     * @return true if there are no interceptors for reading events
     */
    boolean noEventReadInterceptors(String context);

    /**
     * Checks if there aren't any interceptors for reading snapshots.
     *
     * @return true if there are no interceptors for reading snapshots
     */
    default boolean noSnapshotReadInterceptors(String context) {
        return false;
    }
}
