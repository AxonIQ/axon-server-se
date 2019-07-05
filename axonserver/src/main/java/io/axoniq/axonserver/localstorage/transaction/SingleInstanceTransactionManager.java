/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Marc Gathier
 */
public class SingleInstanceTransactionManager implements StorageTransactionManager{
    private final EventStorageEngine eventStorageEngine;

    public SingleInstanceTransactionManager(
            EventStorageEngine eventStorageEngine) {
        this.eventStorageEngine = eventStorageEngine;
    }

    @Override
    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
        return eventStorageEngine.store(eventStorageEngine.prepareTransaction(eventList));
    }

    @Override
    public void reserveSequenceNumbers(List<SerializedEvent> eventList) {
        eventStorageEngine.reserveSequenceNumbers(eventList, false);
    }
}
