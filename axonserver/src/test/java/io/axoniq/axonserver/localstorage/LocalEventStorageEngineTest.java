/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.interceptor.NoOpEventInterceptors;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc Gathier
 */
public class LocalEventStorageEngineTest {
    private LocalEventStore testSubject;
    private static final String SAMPLE_CONTEXT = "FakeContext";
    private List<CompletableFuture<Long>> pendingTransactions = new ArrayList<>();

    @Before
    public void setup() {
        StorageTransactionManagerFactory transactionManagerFactory = eventStore -> new StorageTransactionManager() {

            @Override
            public Mono<Long> storeBatch(List<Event> eventList) {
                return Mono.defer(()-> {
                    CompletableFuture<Long> pendingTransaction = new CompletableFuture<>();
                    pendingTransactions.add(pendingTransaction);
                    return Mono.fromFuture(pendingTransaction);
                });
            }

            @Override
            public Runnable reserveSequenceNumbers(List<Event> eventList) {
                return () -> {
                };
            }

            @Override
            public void cancelPendingTransactions() {
                pendingTransactions.forEach(p -> p
                        .completeExceptionally(new RuntimeException("Transaction cancelled")));
            }

        };
        testSubject = new LocalEventStore(new EventStoreFactory() {
            @Override
            public EventStorageEngine createEventStorageEngine(String context) {
                return new FakeEventStore(EventType.EVENT);
            }

            @Override
            public EventStorageEngine createSnapshotStorageEngine(String context) {
                return new FakeEventStore(EventType.SNAPSHOT);
            }
        }, new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector()),
                                          transactionManagerFactory,
                                          new NoOpEventInterceptors(),
                                          new DefaultEventDecorator(), 1000, 10, 10);
        testSubject.initContext(SAMPLE_CONTEXT, false);
        testSubject.start();
    }

    @Test
    public void deleteContext() {
        testSubject.deleteContext(SAMPLE_CONTEXT, false);
    }

    @Test
    public void cancel() {
        Event event = Event.getDefaultInstance();
        Mono<Void> result = testSubject.appendEvents(SAMPLE_CONTEXT, Flux.just(new SerializedEvent(event)), null);

        StepVerifier.create(result)
                .thenAwait(Duration.ofMillis(100))
                .then(()->{
                    assertEquals(1, pendingTransactions.size());
                    testSubject.cancel(SAMPLE_CONTEXT);
                })
                    .verifyErrorMessage("Transaction cancelled");
        assertTrue(pendingTransactions.get(0).isDone());
    }

    @Test
    public void appendSnapshot() {
        Mono<Void> appendSnapshotOp = testSubject.appendSnapshot(SAMPLE_CONTEXT,
                                                         Event.newBuilder()
                                                              .setAggregateIdentifier(
                                                                      "AGGREGATE_WITH_ONE_EVENT")
                                                              .setAggregateSequenceNumber(0)
                                                              .build(),
                                                         null);

        StepVerifier.create(appendSnapshotOp)
                .thenAwait(Duration.ofMillis(100))
                .then(()-> {
                    if (!pendingTransactions.isEmpty()) {
                        assertEquals(1, pendingTransactions.size());
                        pendingTransactions.get(0).complete(100L);
                    }
                })
                .verifyComplete();

        assertTrue(pendingTransactions.get(0).isDone());
    }

    @Test
    public void appendSnapshotFailsWhenNoEventsFound() throws InterruptedException, TimeoutException {
        Mono<Void> snapshot = testSubject.appendSnapshot(SAMPLE_CONTEXT,
                                                         Event.newBuilder()
                                                              .setAggregateIdentifier(
                                                                      "AGGREGATE_WITH_NO_EVENTS")
                                                              .setAggregateSequenceNumber(0)
                                                              .build(),
                                                         null);
        Thread.sleep(100);
        assertEquals(0, pendingTransactions.size());
        StepVerifier.create(snapshot)
                    .verifyErrorMatches(e -> MessagingPlatformException.class == e.getClass()
                            && ErrorCode.INVALID_SEQUENCE == ((MessagingPlatformException) e).getErrorCode());
    }

    @Test
    public void testAppendEvent() {
        Flux<SerializedEvent> events = Flux.just(new SerializedEvent(Event.getDefaultInstance()));
        Mono<Void> result = testSubject.appendEvents(SAMPLE_CONTEXT, events, null);

        StepVerifier.create(result)
                .thenAwait(Duration.ofMillis(100))
                .then(() -> {
                    if (!pendingTransactions.isEmpty()) {
                        assertEquals(1, pendingTransactions.size());
                        pendingTransactions.get(0).complete(100L);
                    }

                })
                    .verifyComplete();
        assertTrue(pendingTransactions.get(0).isDone());
    }

    @Test
    public void events() {
        GetEventsRequest request1 = GetEventsRequest.newBuilder()
                                                    .setTrackingToken(100)
                                                    .setNumberOfPermits(10)
                                                    .build();
        GetEventsRequest request2 = GetEventsRequest.newBuilder()
                                                    .setNumberOfPermits(10)
                                                    .build();
        Sinks.Many<GetEventsRequest> sink = Sinks.many()
                                                 .unicast()
                                                 .onBackpressureBuffer();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> sink.tryEmitNext(request1), 100, TimeUnit.MILLISECONDS);
        scheduler.schedule(() -> sink.tryEmitNext(request2), 1, TimeUnit.SECONDS);
        scheduler.schedule(sink::tryEmitComplete, 2, TimeUnit.SECONDS);
        StepVerifier.create(testSubject.events(SAMPLE_CONTEXT, null, sink.asFlux()))
                    .expectNextCount(20)
                    .verifyComplete();
    }
}