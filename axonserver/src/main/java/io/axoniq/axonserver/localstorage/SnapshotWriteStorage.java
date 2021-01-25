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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Handles write actions for aggregate snapshots.
 * @author Marc Gathier
 * @since 4.0
 */
public class SnapshotWriteStorage {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotWriteStorage.class);

    private final StorageTransactionManager storageTransactionManager;
    private final Map<String, BiConsumer<Long, Event>> listeners = new ConcurrentHashMap<>();

    public SnapshotWriteStorage(StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Confirmation> store(Event snapshot) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        storageTransactionManager.store(Collections.singletonList(snapshot))
                                 .whenComplete((firstToken, cause) -> {
                                     if (cause == null) {
                                         completableFuture.complete(Confirmation.newBuilder().setSuccess(true).build());
                         if( ! listeners.isEmpty()) {
                    listeners.values()
                             .forEach(consumer -> snapshotStored(consumer, firstToken, snapshot));
                }            } else {
                                         completableFuture.completeExceptionally(cause);
                                     }
                                 });
        return completableFuture;
    }

    private void snapshotStored(
            BiConsumer<Long, Event> consumer,
            Long firstToken, Event event) {
        try {
            consumer.accept(firstToken, event);
        } catch(Exception ex) {
            logger.debug("Listener failed", ex);
        }

    }

    public Registration registerEventListener(BiConsumer<Long, Event> listener) {
        String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return () -> listeners.remove(id);
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
