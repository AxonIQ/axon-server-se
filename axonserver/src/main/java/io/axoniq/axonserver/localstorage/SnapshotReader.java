/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Marc Gathier
 */
public class SnapshotReader {
    private final EventStorageEngine datafileManagerChain;

    private final Supplier<ExecutorService> dataFetcherSchedulerProvider;

    public SnapshotReader(EventStorageEngine datafileManagerChain) {
        this(datafileManagerChain,new DataFetcherSchedulerProvider());
    }

    public SnapshotReader(EventStorageEngine datafileManagerChain,  Supplier<ExecutorService> dataFetcherSchedulerSupplier) {
        this.datafileManagerChain = datafileManagerChain;
        this.dataFetcherSchedulerProvider = dataFetcherSchedulerSupplier;
    }

    public Optional<SerializedEvent> readSnapshot(String aggregateId, long minSequenceNumber, long maxSequenceNumber) {
        return datafileManagerChain
                .getLastEvent(aggregateId, minSequenceNumber, maxSequenceNumber)
                .map(SerializedEvent::asSnapshot);
    }

    public Mono<SerializedEvent> snapshot(String aggregateId, long minSequenceNumber, long maxSequenceNumber) {
        return Mono.fromCallable(()->readSnapshot(aggregateId,minSequenceNumber,maxSequenceNumber))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .subscribeOn(Schedulers.fromExecutorService(dataFetcherSchedulerProvider.get()));
    }

    public void streamByAggregateId(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        datafileManagerChain.processEventsPerAggregateHighestFirst(aggregateId,
                                                                   minSequenceNumber,
                                                                   maxSequenceNumber,
                                                                   maxResults,
                                                                   e -> eventConsumer.accept(e.asSnapshot()));
    }
}
