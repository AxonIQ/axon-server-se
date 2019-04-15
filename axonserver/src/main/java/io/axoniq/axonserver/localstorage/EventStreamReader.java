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
import org.springframework.boot.actuate.health.Health;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public class EventStreamReader {
    private final EventStorageEngine eventStorageEngine;
    private final EventWriteStorage eventWriteStorage;
    private final EventStreamExecutor eventStreamExecutor;

    public EventStreamReader(EventStorageEngine datafileManagerChain,
                             EventWriteStorage eventWriteStorage,
                             EventStreamExecutor eventStreamExecutor) {
        this.eventStorageEngine = datafileManagerChain;
        this.eventWriteStorage = eventWriteStorage;
        this.eventStreamExecutor = eventStreamExecutor;
    }

    public EventStreamController createController(Consumer<SerializedEventWithToken> eventWithTokenConsumer, Consumer<Throwable> errorCallback) {
        return new EventStreamController(eventWithTokenConsumer, errorCallback, eventStorageEngine, eventWriteStorage, eventStreamExecutor);
    }

    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return eventStorageEngine.transactionIterator(firstToken, limitToken);
    }

    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {
        eventStorageEngine.query(minToken, minTimestamp, consumer);
    }

    public long getFirstToken() {
        return eventStorageEngine.getFirstToken();
    }

    public long getTokenAt(long instant) {
        return eventStorageEngine.getTokenAt(instant);
    }

    public void health(Health.Builder builder) {
        eventStorageEngine.health(builder);
    }

    public long getLastToken() {
        return eventStorageEngine.getLastToken();
    }
}
