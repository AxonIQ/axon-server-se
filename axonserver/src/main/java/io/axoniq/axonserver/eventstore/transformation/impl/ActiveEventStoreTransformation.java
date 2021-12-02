/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.eventstore.transformation.impl;

import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.data.util.CloseableIterator;

/**
 * Maintains in-memory state for an active transformation.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ActiveEventStoreTransformation {

    private final String id;
    private final String context;
    private final EventStoreTransformationJpa.Status status;
    private final long lastToken;
    private final CloseableIterator<SerializedEventWithToken> iterator;


    public ActiveEventStoreTransformation(String id, String context, EventStoreTransformationJpa.Status status,
                                          long lastToken) {
        this(id, context, status, lastToken, null);
    }

    public ActiveEventStoreTransformation(String id, String context, EventStoreTransformationJpa.Status status,
                                          long lastToken,
                                          CloseableIterator<SerializedEventWithToken> iterator) {
        this.id = id;
        this.context = context;
        this.status = status;
        this.lastToken = lastToken;
        this.iterator = iterator;
    }

    public String id() {
        return id;
    }

    public String context() {
        return context;
    }

    public long lastToken() {
        return lastToken;
    }

    public boolean applying() {
        return EventStoreTransformationJpa.Status.CLOSED.equals(status);
    }

    public CloseableIterator<SerializedEventWithToken> iterator() {
        return iterator;
    }

    public ActiveEventStoreTransformation withIterator(CloseableIterator<SerializedEventWithToken> iterator) {
        return new ActiveEventStoreTransformation(id, context, status, lastToken, iterator);
    }

    public ActiveEventStoreTransformation withState(EventStoreTransformationJpa transformation) {
        return new ActiveEventStoreTransformation(id, context, status, lastToken, iterator);
    }

    public ActiveEventStoreTransformation withLastToken(long lastToken) {
        return new ActiveEventStoreTransformation(id, context, status, lastToken, iterator);
    }

}
