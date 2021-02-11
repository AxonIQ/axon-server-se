/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.grpc.event.Event;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface for a transaction manager.
 * @author Marc Gathier
 * @since 4.0
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<Event> eventList);

    Runnable reserveSequenceNumbers(List<Event> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void cancelPendingTransactions() {

    }

    /**
     * Deletes all events/snapshots related to the event storage engine managed by this transaction manager.
     */
    void deleteAllEventData();
}
