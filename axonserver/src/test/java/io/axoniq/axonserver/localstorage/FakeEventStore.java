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
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Marc Gathier
 */
public class FakeEventStore implements EventStorageEngine {

    private final EventType eventType;

    public FakeEventStore(EventType eventType) {
        this.eventType = eventType;
    }

    @Override
    public void init(boolean validate, long defaultFirstToken) {

    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint... searchHints) {
        if ("AGGREGATE_WITH_ONE_EVENT".equals(aggregateIdentifier)) {
            return Optional.of(0L);
        }
        return Optional.empty();
    }

    @Override
    public Registration registerCloseListener(Runnable listener) {
        return () -> {};
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber, long maxSequenceNumber) {
        return Optional.empty();
    }

    @Override
    public Flux<SerializedEvent> eventsPerAggregate(String aggregateId, long firstSequenceNumber,
                                                    long lastSequenceNumber, long minToken) {
        return Flux.empty();
    }

    @Override
    public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                          long actualMaxSequenceNumber, long minToken,
                                          Consumer<SerializedEvent> eventConsumer) {

    }

    @Override
    public void processEventsPerAggregateHighestFirst(String aggregateId, long actualMinSequenceNumber,
                                                      long actualMaxSequenceNumber,
                                                      int maxResults, Consumer<SerializedEvent> eventConsumer) {

    }

    @Override
    public EventTypeContext getType() {
        return new EventTypeContext("FakeContext", eventType);
    }

    @Override
    public CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return null;
    }

    @Override
    public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {

    }

    @Override
    public long getFirstToken() {
        return 0;
    }

    @Override
    public long getLastToken() {
        return 10000;
    }

    @Override
    public long nextToken() {
        return 10001;
    }

    @Override
    public long getTokenAt(long instant) {
        return 0;
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        return new CloseableIterator<SerializedEventWithToken>() {
            long sequence = start;
            int remaining = 100;
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public SerializedEventWithToken next() {
                remaining--;
                try {
                    SerializedEventWithToken serializedEventWithToken = new SerializedEventWithToken(sequence,
                                                                                                     Event.newBuilder()
                                                                                                          .setAggregateIdentifier(
                                                                                                                  "aaaa")
                                                                                                          .build());

                    sequence++;
                    return serializedEventWithToken;
                } catch (RuntimeException ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            }
        };
    }
}
