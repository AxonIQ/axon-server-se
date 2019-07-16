/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.SerializedEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the interface for a transaction manager.
 * @author Marc Gathier
 */
public interface StorageTransactionManager {

    CompletableFuture<Long> store(List<SerializedEvent> eventList);

    Runnable reserveSequenceNumbers(List<SerializedEvent> eventList);

    default long waitingTransactions() {
        return 0;
    }

    default void cancelPendingTransactions() {

    }
}
