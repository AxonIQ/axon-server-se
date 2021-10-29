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
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ActiveEventStoreTransformation {

    private final EventStoreTransformationJpa persistedState;
    private long lastToken = -1;
    private final CloseableIterator<SerializedEventWithToken> iterator;


    public ActiveEventStoreTransformation(EventStoreTransformationJpa transformation, long lastToken) {
        this(transformation, lastToken, null);
    }

    public ActiveEventStoreTransformation(EventStoreTransformationJpa persistedState, long lastToken,
                                          CloseableIterator<SerializedEventWithToken> iterator) {
        this.persistedState = persistedState;
        this.lastToken = lastToken;
        this.iterator = iterator;
    }

    public String id() {
        return persistedState.getTransformationId();
    }

    public String context() {
        return persistedState.getContext();
    }

    public long lastToken() {
        return lastToken;
    }

    public boolean applying() {
        return EventStoreTransformationJpa.Status.CLOSED.equals(persistedState.getStatus());
    }

    public CloseableIterator<SerializedEventWithToken> iterator() {
        return iterator;
    }

    public ActiveEventStoreTransformation withIterator(CloseableIterator<SerializedEventWithToken> iterator) {
        return new ActiveEventStoreTransformation(persistedState, lastToken, iterator);
    }

    public ActiveEventStoreTransformation withState(EventStoreTransformationJpa transformation) {
        return new ActiveEventStoreTransformation(transformation, lastToken, iterator);
    }

    public ActiveEventStoreTransformation withLastToken(long lastToken) {
        return new ActiveEventStoreTransformation(persistedState, lastToken, iterator);
    }

}
