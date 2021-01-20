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
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class EventDispatcherTest {

    private EventDispatcher testSubject;
    @Mock
    private EventStore eventStoreClient;

    @Mock
    private EventStoreLocator eventStoreLocator;

    @Mock
    private StreamObserver<InputStream> appendEventConnection;

    @Before
    public void setUp() {
        when(eventStoreClient.createAppendEventConnection(any(), any(), any())).thenReturn(appendEventConnection);
        when(eventStoreLocator.getEventStore(eq("OtherContext"))).thenReturn(null);
        when(eventStoreLocator.getEventStore(eq(Topology.DEFAULT_CONTEXT), anyBoolean())).thenReturn(eventStoreClient);
        when(eventStoreLocator.getEventStore(eq(Topology.DEFAULT_CONTEXT))).thenReturn(eventStoreClient);
        testSubject = new EventDispatcher(eventStoreLocator, () -> Topology.DEFAULT_CONTEXT,
                                          () -> GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                          new MeterFactory(Metrics.globalRegistry,
                                                           new DefaultMetricCollector()));
    }

    @Test
    public void appendEvent() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(responseObserver);
        inputStream.onNext(dummyEvent());
        assertTrue(responseObserver.values().isEmpty());
        inputStream.onCompleted();
        verify( appendEventConnection).onCompleted();
    }

    private InputStream dummyEvent() {
        return new ByteArrayInputStream(Event.newBuilder().build().toByteArray());
    }

    @Test
    public void appendEventRollback() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(responseObserver);
        inputStream.onNext(dummyEvent());
        assertTrue(responseObserver.values().isEmpty());
        inputStream.onError(new Throwable());
        assertTrue(responseObserver.errors().isEmpty());
        verify(appendEventConnection).onError(any());
    }

    @Test
    public void appendSnapshot() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        CompletableFuture<Confirmation> appendFuture = new CompletableFuture<>();
        when(eventStoreClient.appendSnapshot(any(), any(), any(Event.class))).thenReturn(appendFuture);
        testSubject.appendSnapshot(Event.newBuilder().build(), responseObserver);
        appendFuture.complete(Confirmation.newBuilder().build());
        verify(eventStoreClient).appendSnapshot(any(), any(), any(Event.class));
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void listAggregateEventsNoEventStore() {
        testSubject.listAggregateEvents("OtherContext", GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                        GetAggregateEventsRequest.newBuilder().build(),
                                        new FakeStreamObserver<>());
    }

    @Test
    public void listEvents() {
        FakeStreamObserver<InputStream> responseObserver = new FakeStreamObserver<>();
        AtomicReference<StreamObserver<InputStream>> eventStoreOutputStreamRef = new AtomicReference<>();
        StreamObserver<GetEventsRequest> eventStoreResponseStream = new StreamObserver<GetEventsRequest>() {
            @Override
            public void onNext(GetEventsRequest o) {
                StreamObserver<InputStream> responseStream = eventStoreOutputStreamRef.get();
                responseStream.onNext(new ByteArrayInputStream(EventWithToken.newBuilder().build().toByteArray()));
                responseStream.onCompleted();
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        when(eventStoreClient.listEvents(any(), any(), any(StreamObserver.class))).then(a -> {
            eventStoreOutputStreamRef.set((StreamObserver<InputStream>) a.getArguments()[2]);
            return eventStoreResponseStream;
        });
        StreamObserver<GetEventsRequest> inputStream = testSubject.listEvents(responseObserver);
        inputStream.onNext(GetEventsRequest.newBuilder()
                                           .setClientId("sampleClient")
                                           .build());
        verify(eventStoreLocator).getEventStore(Topology.DEFAULT_CONTEXT, false);
        assertEquals(1, responseObserver.values().size());
        responseObserver = new FakeStreamObserver<>();
        inputStream = testSubject.listEvents(responseObserver);
        inputStream.onNext(GetEventsRequest.newBuilder()
                                           .setForceReadFromLeader(true)
                                           .setClientId("sampleClient")
                                           .build());
        verify(eventStoreLocator).getEventStore(Topology.DEFAULT_CONTEXT, true);
        assertEquals(1, responseObserver.completedCount());
        testSubject.on(new TopologyEvents.ApplicationDisconnected(Topology.DEFAULT_CONTEXT,
                                                                  "myComponent",
                                                                  "sampleClient"));
        assertTrue(testSubject.eventTrackerStatus(Topology.DEFAULT_CONTEXT).isEmpty());
    }

    @Test
    public void queryEvents() {
        FakeStreamObserver<QueryEventsResponse> responseObserver = new FakeStreamObserver<>();
        AtomicReference<StreamObserver<QueryEventsResponse>> eventStoreOutputStreamRef = new AtomicReference<>();
        StreamObserver<QueryEventsRequest> eventStoreResponseStream = new StreamObserver<QueryEventsRequest>() {
            @Override
            public void onNext(QueryEventsRequest value) {
                StreamObserver<QueryEventsResponse> responseStream = eventStoreOutputStreamRef.get();
                responseStream.onNext(QueryEventsResponse.newBuilder().build());
                responseStream.onCompleted();
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
        when(eventStoreClient.queryEvents(any(), any(), any(StreamObserver.class))).then(a -> {
            eventStoreOutputStreamRef.set((StreamObserver<QueryEventsResponse>) a.getArguments()[2]);
            return eventStoreResponseStream;
        });
        StreamObserver<QueryEventsRequest> inputStream = testSubject.queryEvents(responseObserver);
        inputStream.onNext(QueryEventsRequest.newBuilder().build());
        verify(eventStoreLocator).getEventStore(Topology.DEFAULT_CONTEXT, false);
        assertEquals(1, responseObserver.values().size());

        responseObserver = new FakeStreamObserver<>();
        inputStream = testSubject.queryEvents(responseObserver);
        inputStream.onNext(QueryEventsRequest.newBuilder()
                                             .setForceReadFromLeader(true)
                                             .build());
        verify(eventStoreLocator).getEventStore(Topology.DEFAULT_CONTEXT, true);
        assertEquals(1, responseObserver.completedCount());
    }
}
