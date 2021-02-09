/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.interceptor.NoOpEventInterceptors;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.*;
import io.axoniq.axonserver.localstorage.query.QueryEventsRequestStreamObserver;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.util.CloseableIterator;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;

/**
 * @author Marc Gathier
 */
public class HttpStreamingQueryTest {
    private HttpStreamingQuery testSubject;

    @Before
    public void setUp() {
        EventStorageEngine engine = new EventStorageEngine() {
            @Override
            public void init(boolean validate, long defaultFirstToken) {

            }

            @Override
            public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint... searchHints) {
                return Optional.empty();
            }

            @Override
            public Registration registerCloseListener(Runnable listener) {
                return () -> {};
            }

            @Override
            public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber) {
                return Optional.empty();
            }

            @Override
            public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                                  long actualMaxSequenceNumber, long minToken,
                                                  Consumer<SerializedEvent> eventConsumer) {

            }

            @Override
            public void processEventsPerAggregateHighestFirst(String aggregateId, long actualMinSequenceNumber,
                                                              long actualMaxSequenceNumber, int maxResults,
                                                              Consumer<SerializedEvent> eventConsumer) {

            }

            @Override
            public EventTypeContext getType() {
                return new EventTypeContext(Topology.DEFAULT_CONTEXT, EventType.EVENT);
            }

            @Override
            public CloseableIterator<SerializedTransactionWithToken> transactionIterator(long firstToken,
                                                                                         long limitToken) {
                return null;
            }

            @Override
            public void query(QueryOptions queryOptions,
                              Predicate<EventWithToken> consumer) {
                Event event = Event.newBuilder().setAggregateIdentifier("demo").build();
                int i = 100000;
                EventWithToken eventWithToken;
                do {
                    i--;
                    eventWithToken = EventWithToken.newBuilder().setToken(i).setEvent(event).build();
                } while (consumer.test(eventWithToken));
            }

            @Override
            public long getFirstToken() {
                return 0;
            }

            @Override
            public long getTokenAt(long instant) {
                return 0;
            }

            @Override
            public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
                return null;
            }

            @Override
            public long nextToken() {
                return 0;
            }

            @Override
            public void deleteAllEventData() {

            }

        };

        LocalEventStore localEventStore = new LocalEventStore(new EventStoreFactory() {
            @Override
            public EventStorageEngine createEventStorageEngine(String context) {
                return engine;
            }

            @Override
            public EventStorageEngine createSnapshotStorageEngine(String context) {
                return engine;
            }
        }, new SimpleMeterRegistry(), eventStore -> null, new NoOpEventInterceptors());
        localEventStore.initContext(Topology.DEFAULT_CONTEXT, false);
        EventStoreLocator eventStoreLocator = new DefaultEventStoreLocator(localEventStore);
        testSubject = new HttpStreamingQuery(eventStoreLocator);
    }

    @Test
    public void query() throws InterruptedException {
        List<Object> messages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);


        SseEmitter emitter = new SseEmitter(5000L) {
            @Override
            public void send(Object object) throws IOException {
                messages.add(messages);
            }

            @Override
            public void send(SseEventBuilder builder) throws IOException {
                messages.add(builder.build());
            }
        };
        emitter.onError(t -> {
            t.printStackTrace();
            latch.countDown();
        });

        emitter.onCompletion(latch::countDown);

        emitter.onTimeout(latch::countDown);
        testSubject.query(Topology.DEFAULT_CONTEXT, null, "aggregateIdentifier contains \"demo\" | limit( 10)",
                          QueryEventsRequestStreamObserver.TIME_WINDOW_CUSTOM, true, false,
                          "token", emitter, false);

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(13, messages.size());
    }
}