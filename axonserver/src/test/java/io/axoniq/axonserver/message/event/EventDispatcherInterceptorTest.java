/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.interceptor.EventInterceptors;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.springframework.security.core.Authentication;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class EventDispatcherInterceptorTest {

    private static final String COMMIT_FAILURE = "commit-error";
    private static final String INTERCEPTOR_FAILURE = "interceptor-error";

    private EventDispatcher testSubject;
    private final AtomicInteger preCommitCount = new AtomicInteger();
    private final AtomicInteger rollbackCount = new AtomicInteger();
    private final AtomicInteger postCommitCount = new AtomicInteger();
    private final AtomicBoolean snapshotPostCommit = new AtomicBoolean();

    @Before
    public void setUp() throws Exception {
        // TODO: move testcases to local event store test
        EventStore eventStore = new EventStore() {
            @Override
            public CompletableFuture<Confirmation> appendSnapshot(String context, Authentication authentication,
                                                                  Event snapshot) {
                CompletableFuture<Confirmation> result = new CompletableFuture<>();
                if (COMMIT_FAILURE.equals(snapshot.getMessageIdentifier())) {
                    result.completeExceptionally(new RuntimeException("Failed on commit"));
                } else {
                    result.complete(Confirmation.newBuilder().setSuccess(true).build());
                }
                return result;
            }

            @Override
            public StreamObserver<InputStream> createAppendEventConnection(String context,
                                                                           Authentication authentication,
                                                                           StreamObserver<Confirmation> responseObserver) {
                List<Event> events = new ArrayList<>();
                return new StreamObserver<InputStream>() {
                    @Override
                    public void onNext(InputStream inputStream) {
                        try {
                            events.add(Event.parseFrom(inputStream));
                        } catch (IOException ioException) {
                            responseObserver.onError(ioException);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        responseObserver.onError(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        if (COMMIT_FAILURE.equals(events.get(0).getMessageIdentifier())) {
                            responseObserver.onError(new RuntimeException("Failed on commit"));
                        } else {
                            responseObserver.onNext(Confirmation.newBuilder().setSuccess(true).build());
                            responseObserver.onCompleted();
                        }
                    }
                };
            }

            @Override
            public void listAggregateEvents(String context, GetAggregateEventsRequest request,
                                            StreamObserver<SerializedEvent> responseStreamObserver) {
                responseStreamObserver.onNext(new SerializedEvent(Event.newBuilder().setSnapshot(true).build()));
                responseStreamObserver.onNext(new SerializedEvent(Event.newBuilder().build()));
                responseStreamObserver.onCompleted();
            }

            @Override
            public StreamObserver<GetEventsRequest> listEvents(String context,
                                                               StreamObserver<InputStream> responseStreamObserver) {
                return new StreamObserver<GetEventsRequest>() {
                    @Override
                    public void onNext(GetEventsRequest getEventsRequest) {
                        EventWithToken eventWithToken = EventWithToken.newBuilder().build();
                        responseStreamObserver.onNext(eventWithToken.toByteString().newInput());
                        responseStreamObserver.onCompleted();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        responseStreamObserver.onError(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        responseStreamObserver.onCompleted();
                    }
                };
            }

            @Override
            public void getFirstToken(String context, GetFirstTokenRequest request,
                                      StreamObserver<TrackingToken> responseObserver) {

            }

            @Override
            public void getLastToken(String context, GetLastTokenRequest request,
                                     StreamObserver<TrackingToken> responseObserver) {

            }

            @Override
            public void getTokenAt(String context, GetTokenAtRequest request,
                                   StreamObserver<TrackingToken> responseObserver) {

            }

            @Override
            public void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                                              StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {

            }

            @Override
            public StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                                  StreamObserver<QueryEventsResponse> responseObserver) {
                return null;
            }

            @Override
            public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                               StreamObserver<SerializedEvent> responseObserver) {

                responseObserver.onNext(new SerializedEvent(Event.newBuilder().build()));
                responseObserver.onCompleted();
            }

            @Override
            public void deleteAllEventData(String context) {

            }
        };
        EventInterceptors testInterceptors = new EventInterceptors() {
            @Override
            public Event appendEvent(Event event, ExtensionUnitOfWork interceptorContext) {
                System.out.println("Append event");
                if (event.getMessageIdentifier().equals(INTERCEPTOR_FAILURE)) {
                    throw new RuntimeException("Failed in interceptor");
                }
                interceptorContext.onFailure(extensionUnitOfWork -> rollbackCount.incrementAndGet());
                return Event.newBuilder(event).putMetaData("intercepted.event", MetaDataValue.newBuilder()
                                                                                             .setTextValue(
                                                                                                     interceptorContext
                                                                                                             .principal())
                                                                                             .build()).build();
            }

            @Override
            public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
                if (snapshot.getMessageIdentifier().equals(INTERCEPTOR_FAILURE)) {
                    throw new RuntimeException(
                            "Failed in interceptor");
                }
                interceptorContext.onFailure(extensionUnitOfWork -> rollbackCount.incrementAndGet());
                return Event.newBuilder(snapshot).putMetaData("intercepted.snapshot", MetaDataValue.newBuilder()
                                                                                                   .setTextValue(
                                                                                                           interceptorContext
                                                                                                                   .principal())
                                                                                                   .build()).build();
            }

            @Override
            public void eventsPreCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
                System.out.println("Events pre commit");
                events.forEach(e -> assertNotNull(e.getMetaDataMap().get("intercepted.event")));
                preCommitCount.set(events.size());
            }

            @Override
            public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
                System.out.println("Events post commit");
                events.forEach(e -> assertNotNull(e.getMetaDataMap().get("intercepted.event")));
                postCommitCount.set(events.size());
            }

            @Override
            public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork interceptorContext) {
                assertNotNull(snapshot.getMetaDataMap().get("intercepted.snapshot"));
                snapshotPostCommit.set(true);
            }

            @Override
            public Event readSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
                return Event.newBuilder(snapshot).putMetaData("snapshot",
                                                              MetaDataValue.newBuilder()
                                                                           .setTextValue(interceptorContext.principal())
                                                                           .build()).build();
            }

            @Override
            public Event readEvent(Event event, ExtensionUnitOfWork interceptorContext) {
                return Event.newBuilder(event).putMetaData("event",
                                                           MetaDataValue.newBuilder()
                                                                        .setTextValue(interceptorContext.principal())
                                                                        .build()).build();
            }

            @Override
            public boolean noReadInterceptors() {
                return false;
            }

            @Override
            public boolean noEventReadInterceptors() {
                return false;
            }
        };
        testSubject = new EventDispatcher(context -> eventStore, () -> Topology.DEFAULT_CONTEXT,
                                          () -> GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL,
                                          testInterceptors, new MeterFactory(Metrics.globalRegistry,
                                                                             new DefaultMetricCollector()));
    }

    //    @Test
    public void appendEvent() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStreamStreamObserver = testSubject.appendEvent(responseObserver);
        inputStreamStreamObserver.onNext(Event.newBuilder().build().toByteString().newInput());
        inputStreamStreamObserver.onCompleted();
        assertEquals(0, responseObserver.errors().size());
        assertEquals(1, responseObserver.values().size());
        assertEquals(1, preCommitCount.get());
        assertEquals(1, postCommitCount.get());
    }

    //    @Test
    public void appendEventCommitFailure() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStreamStreamObserver = testSubject.appendEvent(responseObserver);
        inputStreamStreamObserver.onNext(Event.newBuilder().setMessageIdentifier(COMMIT_FAILURE).build().toByteString()
                                              .newInput());
        inputStreamStreamObserver.onCompleted();
        assertEquals(1, responseObserver.errors().size());
        assertEquals(0, responseObserver.values().size());
        assertEquals(1, preCommitCount.get());
        assertEquals(0, postCommitCount.get());
        assertEquals(1, rollbackCount.get());
    }

    //    @Test
    public void appendEventInterceptorFailure() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<InputStream> inputStreamStreamObserver = testSubject.appendEvent(responseObserver);
        inputStreamStreamObserver.onNext(Event.newBuilder().build().toByteString().newInput());
        inputStreamStreamObserver.onNext(Event.newBuilder().setMessageIdentifier(INTERCEPTOR_FAILURE).build()
                                              .toByteString().newInput());
        inputStreamStreamObserver.onCompleted();
        assertEquals(1, responseObserver.errors().size());
        assertEquals(0, responseObserver.values().size());
        assertEquals(0, preCommitCount.get());
        assertEquals(0, postCommitCount.get());
        assertEquals(1, rollbackCount.get());
    }

    //    @Test
    public void appendSnapshotCommitFailure() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        testSubject.appendSnapshot(Event.newBuilder().setMessageIdentifier(COMMIT_FAILURE).build(), responseObserver);
        assertEquals(1, responseObserver.errors().size());
        assertEquals(0, responseObserver.values().size());
        assertEquals(1, rollbackCount.get());
        assertFalse(snapshotPostCommit.get());
    }

    //    @Test
    public void appendSnapshotInterceptorFailure() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        testSubject.appendSnapshot(Event.newBuilder().setMessageIdentifier(INTERCEPTOR_FAILURE).build(),
                                   responseObserver);
        assertEquals(1, responseObserver.errors().size());
        assertEquals(0, responseObserver.values().size());
        assertEquals(0, rollbackCount.get());
        assertFalse(snapshotPostCommit.get());
    }

    // @Test
    public void appendSnapshot() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        testSubject.appendSnapshot(Event.newBuilder().build(), responseObserver);
        assertEquals(0, responseObserver.errors().size());
        assertEquals(1, responseObserver.values().size());
        assertTrue(snapshotPostCommit.get());
    }

    @Test
    public void listAggregateEvents() {
        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(GetAggregateEventsRequest.getDefaultInstance(), responseObserver);
        assertEquals(0, responseObserver.errors().size());
        assertEquals(2, responseObserver.values().size());
        assertNotNull(responseObserver.values().get(0).asEvent().getMetaDataMap().get("snapshot"));
        assertNotNull(responseObserver.values().get(1).asEvent().getMetaDataMap().get("event"));
    }

    @Test
    public void listEvents() throws IOException {
        FakeStreamObserver<InputStream> responseObserver = new FakeStreamObserver<>();
        StreamObserver<GetEventsRequest> requestStreamObserver = testSubject.listEvents(responseObserver);
        requestStreamObserver.onNext(GetEventsRequest.getDefaultInstance());
        assertEquals(1, responseObserver.values().size());
        EventWithToken serializedEventWithToken = EventWithToken.parseFrom(responseObserver.values().get(0));
        assertNotNull(serializedEventWithToken.getEvent().getMetaDataMap().get("event"));
    }

    @Test
    public void listAggregateSnapshots() {
        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<>();
        testSubject.listAggregateSnapshots(GetAggregateSnapshotsRequest.getDefaultInstance(), responseObserver);
        assertEquals(0, responseObserver.errors().size());
        assertEquals(1, responseObserver.values().size());
        assertNotNull(responseObserver.values().get(0).asEvent().getMetaDataMap().get("snapshot"));
    }
}