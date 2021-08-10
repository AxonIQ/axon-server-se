/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.event.Confirmation;
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
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.axonserver.config.GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;
import static io.axoniq.axonserver.topology.Topology.DEFAULT_CONTEXT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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

    @Mock
    private EventStoreLocator eventStoreLocator;

    private final AtomicReference<MonoSink<Void>> appendEventsResult = new AtomicReference<>();

    @Before
    public void setUp() {
        when(eventStoreClient.appendEvents(any(), any(), any())).thenReturn(Mono.create(appendEventsResult::set));
        when(eventStoreLocator.getEventStore(eq("OtherContext"))).thenReturn(null);
        when(eventStoreLocator.getEventStore(eq(DEFAULT_CONTEXT), anyBoolean())).thenReturn(eventStoreClient);
        when(eventStoreLocator.getEventStore(eq(DEFAULT_CONTEXT))).thenReturn(eventStoreClient);
        testSubject = new EventDispatcher(eventStoreLocator,
                                          new MeterFactory(Metrics.globalRegistry,
                                                           new DefaultMetricCollector()),
                Executors::newCachedThreadPool,
                3,100, 50);
    }

    @Test
    public void appendEvent() {
        FakeStreamObserver<Confirmation> responseObserver = spy(new FakeStreamObserver<>());
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL,
                                                                          responseObserver);
        inputStream.onNext(dummyEvent());
        assertTrue(responseObserver.values().isEmpty());
        inputStream.onCompleted();
        appendEventsResult.get().success();
        verify(responseObserver).onCompleted();
    }

    private InputStream dummyEvent() {
        return new ByteArrayInputStream(Event.newBuilder().build().toByteArray());
    }

    @Test
    public void appendEventRollback() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, responseObserver);
        inputStream.onNext(dummyEvent());
        assertTrue(responseObserver.values().isEmpty());
        Throwable error = new Throwable();
        inputStream.onError(error);
        assertTrue(responseObserver.errors().isEmpty());
    }

    @Test
    public void appendSnapshot() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        when(eventStoreClient.appendSnapshot(any(), any(Event.class), any())).thenReturn(Mono.empty());
        testSubject.appendSnapshot(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, Event.newBuilder().build(), responseObserver);
        verify(eventStoreClient).appendSnapshot(any(), any(Event.class), any());
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void listAggregateEventsNoEventStore() throws ExecutionException, InterruptedException {
        CompletableFuture<Throwable> completableFuture = new CompletableFuture<>();
        testSubject.aggregateEvents("OtherContext", DEFAULT_PRINCIPAL,
                                        GetAggregateEventsRequest.newBuilder().build()).subscribe(e -> {},
                                                                                                  completableFuture::complete,
                                                                                                  () -> completableFuture.completeExceptionally(new RuntimeException("Unexpected success")));
        completableFuture.get();
    }

    @Test
    public void listAggregateEventsWithRetry() throws InterruptedException, ExecutionException, TimeoutException {
        EventStore eventStore = mock(EventStore.class);

        when(eventStore.aggregateEvents(anyString(),any(),any()))
                .thenAnswer(s->{
                    GetAggregateEventsRequest request = s.getArgument(2);
                    long initialSequence = request.getInitialSequence();

                    if(initialSequence == 0) {
                       return  Flux.concat(Flux.range(0,10)
                                .map(i->new SerializedEvent(Event.newBuilder().setAggregateSequenceNumber(i).build())),
                                Flux.error(new RuntimeException("Ups!")));
                    } else {
                        assertEquals ( 10, initialSequence);
                        return Flux.range(Long.valueOf(initialSequence).intValue(),90)
                                .map(i->new SerializedEvent(Event.newBuilder().setAggregateSequenceNumber(i).build()));
                    }
        });

        when(eventStoreLocator.getEventStore(eq("retryContext"))).thenReturn(eventStore);

        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<SerializedEvent>();
        responseObserver.setIsReady(true);

        List<Long> actualSeqNumbers = new ArrayList<>(1000);
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        testSubject.aggregateEvents("retryContext", DEFAULT_PRINCIPAL,
                GetAggregateEventsRequest.newBuilder().setAggregateId("retryAggregateId").build())
                .subscribe(event -> actualSeqNumbers.add(event.getAggregateSequenceNumber()), completableFuture::completeExceptionally, () -> completableFuture.complete(null));

        completableFuture.get(1, TimeUnit.SECONDS);
        List<Long> expectedSeqNumbers = Flux.range(0, 100).map(Long::new).collectList().block();

        assertEquals(expectedSeqNumbers,actualSeqNumbers);
    }

    @Test
    public void listEvents() {
        FakeStreamObserver<InputStream> responseObserver = new FakeStreamObserver<>();
        when(eventStoreClient.events(any(), any(), any(Flux.class)))
                .thenReturn(Flux.just(new SerializedEventWithToken(EventWithToken.getDefaultInstance())));
        StreamObserver<GetEventsRequest> inputStream = testSubject.listEvents(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, responseObserver);
        inputStream.onNext(GetEventsRequest.newBuilder()
                                           .setClientId("sampleClient")
                                           .build());
        verify(eventStoreLocator).getEventStore(DEFAULT_CONTEXT, false);
        assertEquals(1, responseObserver.values().size());
        responseObserver = new FakeStreamObserver<>();
        inputStream = testSubject.listEvents(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, responseObserver);
        inputStream.onNext(GetEventsRequest.newBuilder()
                                           .setForceReadFromLeader(true)
                                           .setClientId("sampleClient")
                                           .build());
        verify(eventStoreLocator).getEventStore(DEFAULT_CONTEXT, true);
        assertEquals(1, responseObserver.completedCount());
        testSubject.on(new TopologyEvents.ApplicationDisconnected(DEFAULT_CONTEXT,
                                                                  "myComponent",
                                                                  "sampleClient"));
        assertTrue(testSubject.eventTrackerStatus(DEFAULT_CONTEXT).isEmpty());
    }


    @Test
    public void queryEvents() {
        FakeStreamObserver<QueryEventsResponse> responseObserver = new FakeStreamObserver<>();
        AtomicReference<StreamObserver<QueryEventsResponse>> eventStoreOutputStreamRef = new AtomicReference<>();
        when(eventStoreClient.queryEvents(any(), any(Flux.class), any()))
                .thenReturn(Flux.just(QueryEventsResponse.getDefaultInstance()));
        StreamObserver<QueryEventsRequest> inputStream = testSubject.queryEvents(DEFAULT_CONTEXT,
                                                                                 DEFAULT_PRINCIPAL,
                                                                                 responseObserver);
        inputStream.onNext(QueryEventsRequest.newBuilder().build());
        verify(eventStoreLocator).getEventStore(DEFAULT_CONTEXT, false);
        assertEquals(1, responseObserver.values().size());

        responseObserver = new FakeStreamObserver<>();
        inputStream = testSubject.queryEvents(DEFAULT_CONTEXT, DEFAULT_PRINCIPAL, responseObserver);
        inputStream.onNext(QueryEventsRequest.newBuilder()
                                             .setForceReadFromLeader(true)
                                             .build());
        verify(eventStoreLocator).getEventStore(DEFAULT_CONTEXT, true);
        assertEquals(1, responseObserver.completedCount());
    }
}
