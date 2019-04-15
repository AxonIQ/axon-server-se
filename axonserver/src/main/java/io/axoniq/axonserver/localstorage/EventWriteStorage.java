/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
 */
public class EventWriteStorage {
    private static final Logger logger = LoggerFactory.getLogger(EventWriteStorage.class);

    private final Map<String, Consumer<SerializedEventWithToken>> listeners = new ConcurrentHashMap<>();
    private final StorageTransactionManager storageTransactionManager;


    public EventWriteStorage( StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Void> store(List<SerializedEvent> eventList) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            validate(eventList);

            storageTransactionManager.store(eventList).whenComplete((firstToken, cause) -> {
                if( cause == null) {
                    completableFuture.complete(null);

                    if( ! listeners.isEmpty()) {
                        IntStream.range(0, eventList.size())
                                 .forEach(i -> {
                                     SerializedEventWithToken event = new SerializedEventWithToken(firstToken + i,
                                                                                                   eventList.get(i));
                                     listeners.values()
                                              .forEach(consumer -> safeForwardEvent(consumer, event));
                                 });
                    }
                } else {
                    completableFuture.completeExceptionally(cause);
                }
            });
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }
        return completableFuture;
    }

    private void safeForwardEvent(Consumer<SerializedEventWithToken> consumer, SerializedEventWithToken event) {
        try {
            consumer.accept(event);
        } catch( RuntimeException re) {
            logger.warn("Failed to forward event", re);
        }
    }

    private void validate(List<SerializedEvent> eventList) {
        storageTransactionManager.reserveSequenceNumbers(eventList);
    }

    public Registration registerEventListener(Consumer<SerializedEventWithToken> listener) {
        String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return () -> listeners.remove(id);
    }


    public long waitingTransactions() {
        return storageTransactionManager.waitingTransactions();
    }

    public void cancelPendingTransactions() {
        storageTransactionManager.cancelPendingTransactions();
    }
}
