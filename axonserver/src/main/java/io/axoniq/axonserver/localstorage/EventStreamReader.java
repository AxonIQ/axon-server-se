/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;

import java.util.function.Predicate;

/**
 * Access to operations for tracking event processors. One instance per context and type (event/snapshot).
 * @author Marc Gathier
 */
public class EventStreamReader {

    private final EventStorageEngine eventStorageEngine;

    public EventStreamReader(EventStorageEngine datafileManagerChain) {
        this.eventStorageEngine = datafileManagerChain;
    }

    public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
        eventStorageEngine.query(queryOptions, consumer);
    }

    /**
     * Returns the first token in the event store for the current context. Returns -1 if event store is empty.
     *
     * @return the first token in this event store
     */
    public long getFirstToken() {
        return eventStorageEngine.getFirstToken();
    }

    /**
     * Returns the token for the first event at or after the specified instant.
     * @param instant timestamp to check
     * @return the token
     */
    public long getTokenAt(long instant) {
        return eventStorageEngine.getTokenAt(instant);
    }

    /**
     * Returns the last token in the event store.
     * @return the last token
     */
    public long getLastToken() {
        return eventStorageEngine.getLastToken();
    }
}
