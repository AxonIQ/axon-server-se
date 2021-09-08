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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class EventStoreTransformation {

    private final EventStoreTransformationJpa wrapped;
    private long previousToken = -1;
    private boolean applying;
    private final AtomicReference<CloseableIterator<SerializedEventWithToken>> iterator = new AtomicReference<>();

    public EventStoreTransformation(
            EventStoreTransformationJpa wrapped) {
        this.wrapped = wrapped;
    }

    public String getId() {
        return wrapped.getTransformationId();
    }


    public String getName() {
        return wrapped.getContext();
    }

    public long getPreviousToken() {
        return previousToken;
    }

    public void setPreviousToken(long previousToken) {
        this.previousToken = previousToken;
    }

    public boolean isApplying() {
        return applying;
    }

    public void setApplying(boolean applying) {
        this.applying = applying;
    }

    public CloseableIterator<SerializedEventWithToken> getIterator() {
        return iterator.get();
    }

    public void setIterator(
            CloseableIterator<SerializedEventWithToken> iterator) {
        this.iterator.set(iterator);
    }

    public void closeIterator() {
        CloseableIterator<SerializedEventWithToken> activeIterator = iterator.getAndSet(null);
        if( activeIterator != null) {
            activeIterator.close();
        }
    }
}
