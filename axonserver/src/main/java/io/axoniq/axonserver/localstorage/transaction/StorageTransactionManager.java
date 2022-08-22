/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<Event> eventList);

    /**
     * Reserves the sequence numbers accordingly to the specified events and returns a {@link Runnable} to restore
     * the previous situation.
     *
     * @param eventList the list of events to reserve the sequence number for
     * @return a {@link Runnable} to restore the previous situation, releasing the reserved sequences
     */
    Runnable reserveSequenceNumbers(List<Event> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void cancelPendingTransactions() {

    }

    default void clearSequenceNumberCache() {

    }
}
