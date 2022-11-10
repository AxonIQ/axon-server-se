/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.micrometer.core.instrument.Metrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.axonserver.config.GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Stefan Dragisic
 */
@RunWith(MockitoJUnitRunner.class)
public class EventDispatcherTest {

    private EventDispatcher testSubject;
    @Mock
    private EventStore eventStoreClient;

    private final AtomicInteger eventStoreWithoutLeaderCalls = new AtomicInteger();
    private final Map<String, EventStore> otherContexts = new HashMap<>();

    private final EventStoreLocator eventStoreLocator = new EventStoreLocator() {
        @Override
        public EventStore getEventStore(String context) {
            if (DEFAULT_CONTEXT.equals(context)) {
                return eventStoreClient;
            }
            return otherContexts.get(context);
        }

        @Override
        public Mono<EventStore> eventStore(String context) {
            if (DEFAULT_CONTEXT.equals(context)) {
                return Mono.just(eventStoreClient);
            }
            EventStore other = otherContexts.get(context);
            return other == null ? Mono.error(new RuntimeException("Not found")) : Mono.just(other);
        }

        @Override
        public EventStore getEventStore(String context, boolean forceLeader) {
            if (!forceLeader) {
                eventStoreWithoutLeaderCalls.incrementAndGet();
            }
            return getEventStore(context);
        }

        @Override
        public Mono<EventStore> eventStore(String context, boolean forceLeader) {
            if (!forceLeader) {
                eventStoreWithoutLeaderCalls.incrementAndGet();
            }
            return eventStore(context);
        }
    };

    @Before
    public void setUp() {
        when(eventStoreClient.appendEvents(any(), any(), any())).then(invocationOnMock -> {
            return Mono.create(sink -> {
                Flux<SerializedEvent> flux = invocationOnMock.getArgument(1);
                flux.subscribe(event -> {}, error -> {
                    System.out.println("Cancelled by client");
                    sink.success();
                }, sink::success);
            });
        });
        testSubject = new EventDispatcher(eventStoreLocator,
                                          new MeterFactory(Metrics.globalRegistry,
                                                           new DefaultMetricCollector()),
                                          3, 100, 50, 30_000);
    }

    @Test
    public void appendEvent() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        Sinks.Many<SerializedEvent> sink = Sinks.many().unicast().onBackpressureBuffer();
        testSubject.appendEvent(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL,
                                                      sink.asFlux())
                .subscribe(r -> {}, e -> completableFuture.completeExceptionally(e), () -> completableFuture.complete(null));
        sink.tryEmitNext(dummyEvent()).orThrow();
        assertFalse(completableFuture.isDone());
        sink.tryEmitComplete().orThrow();
        assertTrue(completableFuture.isDone());
    }

    private SerializedEvent dummyEvent() {
        return new SerializedEvent(Event.newBuilder().build());
    }

    @Test
    public void appendEventRollback() throws ExecutionException, InterruptedException {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        Sinks.Many<SerializedEvent> sink = Sinks.many().unicast().onBackpressureBuffer();
        BaseSubscriber<Void> subscriber = new BaseSubscriber<Void>() {
            @Override
            protected void hookOnComplete() {
                completableFuture.complete(null);
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        }; testSubject.appendEvent(DEFAULT_CONTEXT,
                                                                          DEFAULT_PRINCIPAL,

                                sink.asFlux())
                   .subscribe(subscriber);
        sink.tryEmitNext(dummyEvent()).orThrow();
        sink.tryEmitError(new Throwable("stop"));
        completableFuture.get();
    }

    @Test
    public void appendSnapshot() {
        when(eventStoreClient.appendSnapshot(any(), any(Event.class), any())).thenReturn(Mono.empty());
        StepVerifier.create(testSubject.appendSnapshot(DEFAULT_CONTEXT, Event.getDefaultInstance(), DEFAULT_PRINCIPAL))
                    .verifyComplete();
        verify(eventStoreClient).appendSnapshot(any(), any(Event.class), any());
    }

    @Test
    public void listAggregateEventsNoEventStore() throws ExecutionException, InterruptedException {
        CompletableFuture<Throwable> completableFuture = new CompletableFuture<>();
        testSubject.aggregateEvents("OtherContext", DEFAULT_PRINCIPAL,
                                    GetAggregateEventsRequest.newBuilder().build()).subscribe(e -> {
                                                                                              },
                                                                                              completableFuture::complete,
                                                                                              () -> completableFuture.completeExceptionally(
                                                                                                      new RuntimeException(
                                                                                                              "Unexpected success")));
        completableFuture.get();
    }

    @Test
    public void listAggregateEventsWithRetry() throws InterruptedException, ExecutionException, TimeoutException {
        EventStore eventStore = mock(EventStore.class);

        when(eventStore.aggregateEvents(anyString(), any(), any()))
                .thenAnswer(s -> {
                    GetAggregateEventsRequest request = s.getArgument(2);
                    long initialSequence = request.getInitialSequence();

                    if (initialSequence == 0) {
                        return Flux.concat(Flux.range(0, 10)
                                               .map(i -> new SerializedEvent(Event.newBuilder()
                                                                                  .setAggregateSequenceNumber(i)
                                                                                  .build())),
                                           Flux.error(new RuntimeException("Ups!")));
                    } else {
                        assertEquals(10, initialSequence);
                        return Flux.range(Long.valueOf(initialSequence).intValue(), 90)
                                   .map(i -> new SerializedEvent(Event.newBuilder().setAggregateSequenceNumber(i)
                                                                      .build()));
                    }
                });

        otherContexts.put("retryContext", eventStore);

        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<>();
        responseObserver.setIsReady(true);

        List<Long> actualSeqNumbers = new ArrayList<>(1000);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        testSubject.aggregateEvents("retryContext", DEFAULT_PRINCIPAL,
                                    GetAggregateEventsRequest.newBuilder().setAggregateId("retryAggregateId").build())
                   .subscribe(event -> actualSeqNumbers.add(event.getAggregateSequenceNumber()),
                              completableFuture::completeExceptionally,
                              () -> completableFuture.complete(null));

        completableFuture.get(1, TimeUnit.SECONDS);
        List<Long> expectedSeqNumbers = Flux.range(0, 100).map(Long::new).collectList().block();

        assertEquals(expectedSeqNumbers, actualSeqNumbers);
    }

    @Test
    public void listAggregateEventsWithFailedRetry() throws InterruptedException, TimeoutException {
        EventStore eventStore = mock(EventStore.class);

        when(eventStore.aggregateEvents(anyString(), any(), any()))
                .thenAnswer(s -> Flux.error(new IllegalStateException("Ups!")));

        otherContexts.put("retryContext", eventStore);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        testSubject.aggregateEvents("retryContext", DEFAULT_PRINCIPAL,
                                    GetAggregateEventsRequest.newBuilder().setAggregateId("retryAggregateId").build())
                   .subscribe(event -> {},
                              completableFuture::completeExceptionally,
                              () -> completableFuture.complete(null));

        try {
            completableFuture.get(1, TimeUnit.SECONDS);
            fail("Expecting exception after max retries");
        } catch (ExecutionException executionException) {
            assertTrue( executionException.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void events() {
        when(eventStoreClient.events(any(), any(), any(Flux.class)))
                .thenReturn(Flux.just(new SerializedEventWithToken(EventWithToken.getDefaultInstance())));

        Flux<GetEventsRequest> requestFlux = Flux.just(GetEventsRequest.newBuilder()
                                                                        .setClientId("sampleClient")
                                                                        .build());
        StepVerifier.create(testSubject.events(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, requestFlux))
                    .expectNextCount(1L)
                    .verifyComplete();
        assertEquals(1, eventStoreWithoutLeaderCalls.get());

        requestFlux = Flux.just(GetEventsRequest.newBuilder()
                                                .setForceReadFromLeader(true)
                                                .setClientId("sampleClient")
                                                .build());
        StepVerifier.create(testSubject.events(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, requestFlux))
                    .expectNextCount(1L)
                    .verifyComplete();
        assertEquals(1, eventStoreWithoutLeaderCalls.get());
    }


    @Test
    public void queryEvents() {
        when(eventStoreClient.queryEvents(any(), any(Flux.class), any()))
                .thenReturn(Flux.just(QueryEventsResponse.getDefaultInstance()));
        StepVerifier.create(testSubject.queryEvents(DEFAULT_CONTEXT,
                                                    DEFAULT_PRINCIPAL,
                                                    Flux.just(QueryEventsRequest.newBuilder()
                                                                                .build())))
                .expectNextCount(1L)
                .verifyComplete();
        assertEquals(1, eventStoreWithoutLeaderCalls.get());

        StepVerifier.create(testSubject.queryEvents(DEFAULT_CONTEXT,
                                                    DEFAULT_PRINCIPAL,
                                                    Flux.just(QueryEventsRequest.newBuilder()
                                                                      .setForceReadFromLeader(true)
                                                                                .build())))
                    .expectNextCount(1L)
                    .verifyComplete();
        assertEquals(1, eventStoreWithoutLeaderCalls.get());
    }

    @Test
    public void testTimeoutOnListAggregateEvents() throws InterruptedException {
        Flux<SerializedEvent> flux = Flux.create(sink -> {});
        when(eventStoreClient.aggregateEvents(any(), any(), any())).thenReturn(flux);
        EventDispatcher eventDispatcher = new EventDispatcher(eventStoreLocator,
                                                              new MeterFactory(Metrics.globalRegistry,
                                                                               new DefaultMetricCollector()),
                                                              3, 100, 50, 3);

        StepVerifier.create(eventDispatcher.aggregateEvents(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL,GetAggregateEventsRequest.newBuilder().build()))
                .expectError(MessagingPlatformException.class)
                .verify();
    }
}
