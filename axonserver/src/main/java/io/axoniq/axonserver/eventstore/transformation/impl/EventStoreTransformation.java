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

    private final String id;
    private final String context;
    private long previousToken = -1;
    private boolean applying;
    private final AtomicReference<CloseableIterator<SerializedEventWithToken>> iterator = new AtomicReference<>();

    public EventStoreTransformation(String id, String context) {
        this.id = id;
        this.context = context;
    }

    public String id() {
        return id;
    }

    public String context() {
        return context;
    }

    public long previousToken() {
        return previousToken;
    }

    public void previousToken(long previousToken) {
        this.previousToken = previousToken;
    }

    public boolean applying() {
        return applying;
    }

    public void applying(boolean applying) {
        this.applying = applying;
    }

    public CloseableIterator<SerializedEventWithToken> iterator() {
        return iterator.get();
    }

    public void iterator(
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
