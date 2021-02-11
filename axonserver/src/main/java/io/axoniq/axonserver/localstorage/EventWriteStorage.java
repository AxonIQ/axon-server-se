/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Handles write actions for events.
 * @author Marc Gathier
 * @since 4.0
 */
public class EventWriteStorage {
    private static final Logger logger = LoggerFactory.getLogger(EventWriteStorage.class);

    private final Map<String, BiConsumer<Long, List<Event>>> listeners = new ConcurrentHashMap<>();
    private final StorageTransactionManager storageTransactionManager;


    public EventWriteStorage( StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Void> store(List<Event> eventList) {
        if (eventList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        AtomicReference<Runnable> unreserve = new AtomicReference<>(() -> {
        });
        try {
            unreserve.set(reserveSequences(eventList));
            storageTransactionManager.store(eventList).whenComplete((firstToken, cause) -> {
                if (cause == null) {
                    completableFuture.complete(null);

                    if( ! listeners.isEmpty()) {
                        listeners.values()
                                 .forEach(consumer -> eventsStored(consumer, firstToken, eventList));
                    }
                } else {
                    completableFuture.completeExceptionally(cause);
                    unreserve.get().run();
                }
            });
        } catch (RuntimeException cause) {
            unreserve.get().run();
            completableFuture.completeExceptionally(cause);
        }
        return completableFuture;
    }

    private void eventsStored(
            BiConsumer<Long, List<Event>> consumer,
            Long firstToken, List<Event> eventList) {
        try {
            consumer.accept(firstToken, eventList);
        } catch (Exception ex) {
            logger.debug("Listener failed", ex);
        }
    }

    private Runnable reserveSequences(List<Event> eventList) {
        return storageTransactionManager.reserveSequenceNumbers(eventList);
    }

    public Registration registerEventListener(BiConsumer<Long, List<Event>> listener) {
        String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return () -> listeners.remove(id);
    }

    /**
     * Returns the number of transactions in progress for appending events.
     * @return number of transactions
     */
    public long waitingTransactions() {
        return storageTransactionManager.waitingTransactions();
    }

    public void cancelPendingTransactions() {
        storageTransactionManager.cancelPendingTransactions();
    }

    /**
     * Deletes all events from the event store for this context. Delegates to the transaction manager, so it can clean
     * up its data.
     */
    public void deleteAllEventData() {
        storageTransactionManager.deleteAllEventData();
    }
}
