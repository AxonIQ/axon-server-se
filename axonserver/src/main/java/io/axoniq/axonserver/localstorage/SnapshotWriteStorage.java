/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Handles write actions for aggregate snapshots.
 * @author Marc Gathier
 * @since 4.0
 */
public class SnapshotWriteStorage {

    private final StorageTransactionManager storageTransactionManager;

    public SnapshotWriteStorage(StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Confirmation> store( Event eventMessage) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        storageTransactionManager.store(Collections.singletonList(new SerializedEvent(eventMessage)))
                                 .whenComplete((firstToken, cause) ->  {
            if( cause == null ) {
                completableFuture.complete(Confirmation.newBuilder().setSuccess(true).build());
            } else {
                completableFuture.completeExceptionally(cause);
            }
        });
        return completableFuture;
    }

    /**
     * Returns the number of transactions in progress for appending snapshots.
     * @return number of transactions
     */
    public long waitingTransactions() {
        return storageTransactionManager.waitingTransactions();
    }

    /**
     * Deletes all snapshots from the event store for this context. Delegates to the transaction manager, so it can clean
     * up its data.
     */
    public void deleteAllEventData() {
        storageTransactionManager.deleteAllEventData();
    }
}
