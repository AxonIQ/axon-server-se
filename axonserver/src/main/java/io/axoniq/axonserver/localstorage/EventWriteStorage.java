/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Handles write actions for events.
 * @author Marc Gathier
 * @since 4.0
 */
public class EventWriteStorage {
    private static final Logger logger = LoggerFactory.getLogger(EventWriteStorage.class);

    private final StorageTransactionManager storageTransactionManager;


    public EventWriteStorage( StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public Mono<Void> storeBatch(List<Event> batch) {
        return Mono.just(batch)
                .filter(b -> !b.isEmpty())
                .flatMap(eventList -> {
                    Runnable releaseSequences = reserveSequences(eventList);
                    return storageTransactionManager.storeBatch(eventList)
                            .doOnError(t-> logger.error("Error occurred while writing batch: ",t))
                            .doOnError(e -> releaseSequences.run());
                }).then();
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

    public void clearSequenceNumberCache() {
        storageTransactionManager.clearSequenceNumberCache();
    }
}
