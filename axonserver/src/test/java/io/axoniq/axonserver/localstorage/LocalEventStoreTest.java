/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.interceptor.EventInterceptors;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class LocalEventStoreTest {

    private final CountingEventInterceptors eventInterceptors = new CountingEventInterceptors();
    private LocalEventStore testSubject;

    @Before
    public void setUp() throws Exception {
        Event[] events = {
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(0)
                     .setTimestamp(System.currentTimeMillis())
                        .build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(1).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(2).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(3).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(4).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(5).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(6).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(7).build(),
        };

        Event[] snapshots = {
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(1).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(2).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(4).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(6).build(),
        };
        EventStoreFactory eventStoreFactory = new EventStoreFactory() {
            @Override
            public EventStorageEngine createEventStorageEngine(String context) {
                return new TestEventStore(EventType.EVENT, events);
            }

            @Override
            public EventStorageEngine createSnapshotStorageEngine(String context) {
                return new TestEventStore(EventType.SNAPSHOT, snapshots);
            }
        };

        StorageTransactionManagerFactory transactionManagerFactory = eventStore -> new StorageTransactionManager() {
            @Override
            public CompletableFuture<Long> store(List<Event> eventList) {
                return eventStore.store(eventList);
            }

            @Override
            public Runnable reserveSequenceNumbers(List<Event> eventList) {
                return () -> {
                };
            }

            @Override
            public void deleteAllEventData() {

            }
        };

        testSubject = new LocalEventStore(eventStoreFactory,
                                          new SimpleMeterRegistry(),
                                          transactionManagerFactory,
                                          eventInterceptors);
        testSubject.initContext("demo", false);
        testSubject.start();
    }

    @Test
    public void appendSnapshot() throws ExecutionException, InterruptedException {
        Confirmation result = testSubject.appendSnapshot("demo",
                                                         null,
                                                         Event.newBuilder().build())
                                         .get();
        assertEquals(1, eventInterceptors.appendSnapshot);
        assertEquals(1, eventInterceptors.snapshotPostCommit);
    }

    @Test
    public void appendSnapshotCompensate() throws InterruptedException {
        try {
            testSubject.appendSnapshot("demo",
                                       null,
                                       Event.newBuilder().setMessageIdentifier("FAIL").build())
                       .get();
            fail("Expecting exception");
        } catch (ExecutionException executionException) {
            assertEquals(1, eventInterceptors.appendSnapshot);
            assertEquals(0, eventInterceptors.snapshotPostCommit);
            assertEquals(1, eventInterceptors.compensations.size());
            assertEquals("Compensate Append Snapshot", eventInterceptors.compensations.get(0));
        }
    }

    @Test
    public void createAppendEventConnection() throws ExecutionException, InterruptedException {
        CompletableFuture<Confirmation> result = new CompletableFuture<>();
        StreamObserver<InputStream> eventStream =
                testSubject.createAppendEventConnection("demo",
                                                        null,
                                                        new FutureStreamObserver(result));
        eventStream.onNext(Event.getDefaultInstance().toByteString().newInput());
        eventStream.onNext(Event.getDefaultInstance().toByteString().newInput());
        eventStream.onNext(Event.getDefaultInstance().toByteString().newInput());
        assertEquals(3, eventInterceptors.appendEvent);
        assertEquals(0, eventInterceptors.eventsPreCommit);
        eventStream.onCompleted();
        result.get();
        assertEquals(1, eventInterceptors.eventsPreCommit);
        assertEquals(1, eventInterceptors.eventsPostCommit);
    }

    @Test
    public void createAppendEventConnectionCompensateAppendEntries() throws InterruptedException {
        CompletableFuture<Confirmation> result = new CompletableFuture<>();
        eventInterceptors.failPreCommit = true;
        StreamObserver<InputStream> eventStream =
                testSubject.createAppendEventConnection("demo",
                                                        null,
                                                        new FutureStreamObserver(result));
        eventStream.onNext(Event.newBuilder().setMessageIdentifier("FAIL").build().toByteString().newInput());
        eventStream.onNext(Event.newBuilder().setMessageIdentifier("FAIL").build().toByteString().newInput());
        eventStream.onCompleted();
        try {
            result.get();
            fail("Empty transaction should fail");
        } catch (ExecutionException executionException) {
            assertEquals(2, eventInterceptors.appendEvent);
            assertEquals(0, eventInterceptors.eventsPreCommit);
            assertEquals(2, eventInterceptors.compensations.size());
            assertEquals("Compensate Append Event", eventInterceptors.compensations.get(0));
            assertEquals("Compensate Append Event", eventInterceptors.compensations.get(1));
        }
    }

    @Test
    public void createAppendEventConnectionCompensatePreCommitAndAppendEntry() throws InterruptedException {
        CompletableFuture<Confirmation> result = new CompletableFuture<>();
        StreamObserver<InputStream> eventStream =
                testSubject.createAppendEventConnection("demo",
                                                        null,
                                                        new FutureStreamObserver(result));
        eventStream.onNext(Event.newBuilder().setMessageIdentifier("FAIL").build().toByteString().newInput());
        eventStream.onCompleted();
        try {
            result.get();
            fail("Empty transaction should fail");
        } catch (ExecutionException executionException) {
            assertEquals(1, eventInterceptors.eventsPreCommit);
            assertEquals(2, eventInterceptors.compensations.size());
            assertEquals("Compensate Events Pre Commit", eventInterceptors.compensations.get(0));
            assertEquals("Compensate Append Event", eventInterceptors.compensations.get(1));
        }
    }

    @Test
    public void listAggregateEvents() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> events = new FakeStreamObserver<>();
        testSubject.listAggregateEvents("demo", null,
                                        GetAggregateEventsRequest.newBuilder()
                                                                 .setAllowSnapshots(true)
                                                                 .build(),
                                        events);

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, events.completedCount()));
        assertEquals(1, eventInterceptors.readSnapshot);
        assertEquals(1, eventInterceptors.readEvent);

        assertTrue(events.values().get(0).isSnapshot());
        assertFalse(events.values().get(1).isSnapshot());
    }

    @Test
    public void listAggregateEventsNoSnapshots() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> events = new FakeStreamObserver<>();
        testSubject.listAggregateEvents("demo", null,
                                        GetAggregateEventsRequest.newBuilder()
                                                                 .build(),
                                        events);

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, events.completedCount()));
        assertEquals(0, eventInterceptors.readSnapshot);
        assertEquals(8, eventInterceptors.readEvent);

        assertFalse(events.values().get(0).isSnapshot());
        assertFalse(events.values().get(1).isSnapshot());
    }

    @Test
    public void listAggregateSnapshots() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> events = new FakeStreamObserver<>();
        testSubject.listAggregateSnapshots("demo", null,
                                           GetAggregateSnapshotsRequest.newBuilder()
                                                                       .build(),
                                           events);

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, events.completedCount()));
        assertEquals(4, eventInterceptors.readSnapshot);
        assertEquals(0, eventInterceptors.readEvent);
    }

    @Test
    public void listEvents() throws InterruptedException {
        FakeStreamObserver<InputStream> events = new FakeStreamObserver<>();
        StreamObserver<GetEventsRequest> requestStream = testSubject.listEvents("demo",
                                                                                null,
                                                                                events);
        requestStream.onNext(GetEventsRequest.newBuilder()
                                             .setNumberOfPermits(100)
                                             .build());
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(8, eventInterceptors.readEvent));
    }

    @Test
    public void queryEvents() throws InterruptedException {
        FakeStreamObserver<QueryEventsResponse> events = new FakeStreamObserver<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(
                "demo",
                null,
                events);

        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setNumberOfPermits(100)
                                               .setQuery("limit(100)")
                                               .build());
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(8, eventInterceptors.readEvent));
    }

    private static class FutureStreamObserver implements StreamObserver<Confirmation> {

        private final CompletableFuture<Confirmation> result;

        public FutureStreamObserver(CompletableFuture<Confirmation> result) {
            this.result = result;
        }

        @Override
        public void onNext(Confirmation o) {
            result.complete(o);
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {

        }
    }

    private class CountingEventInterceptors implements EventInterceptors {

        boolean failAppend;
        boolean failPreCommit;
        int appendEvent;
        int appendSnapshot;
        int eventsPreCommit;
        int eventsPostCommit;
        int snapshotPostCommit;
        int readEvent;
        int readSnapshot;
        List<String> compensations = new ArrayList<>();

        @Override
        public Event appendEvent(Event event, ExtensionUnitOfWork interceptorContext) {
            if (failAppend) {
                throw new RuntimeException("appendEvent");
            }
            interceptorContext.onFailure(e -> compensations.add("Compensate Append Event"));
            appendEvent++;
            return event;
        }

        @Override
        public Event appendSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
            if (failAppend) {
                throw new RuntimeException("appendSnapshot");
            }
            interceptorContext.onFailure(e -> compensations.add("Compensate Append Snapshot"));
            appendSnapshot++;
            return snapshot;
        }

        @Override
        public void eventsPreCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
            if (failPreCommit) {
                throw new RuntimeException("eventsPreCommit");
            }
            interceptorContext.onFailure(e -> compensations.add("Compensate Events Pre Commit"));
            eventsPreCommit++;
        }

        @Override
        public void eventsPostCommit(List<Event> events, ExtensionUnitOfWork interceptorContext) {
            eventsPostCommit++;
        }

        @Override
        public void snapshotPostCommit(Event snapshot, ExtensionUnitOfWork interceptorContext) {
            snapshotPostCommit++;
        }

        @Override
        public Event readSnapshot(Event snapshot, ExtensionUnitOfWork interceptorContext) {
            readSnapshot++;
            return snapshot;
        }

        @Override
        public Event readEvent(Event event, ExtensionUnitOfWork interceptorContext) {
            readEvent++;
            return event;
        }

        @Override
        public boolean noReadInterceptors(String context) {
            return false;
        }

        @Override
        public boolean noEventReadInterceptors(String context) {
            return false;
        }

        @Override
        public boolean noSnapshotReadInterceptors(String context) {
            return false;
        }
    }

    private static class TestEventStore extends FakeEventStore {

        private final Event[] events;

        public TestEventStore(EventType eventType, Event[] events) {
            super(eventType);
            this.events = events;
        }

        @Override
        public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                              long actualMaxSequenceNumber, long minToken,
                                              Consumer<SerializedEvent> eventConsumer) {
            Arrays.stream(events).filter(e -> e.getAggregateSequenceNumber() >= actualMinSequenceNumber)
                  .forEach(e -> eventConsumer.accept(new SerializedEvent(e)));
        }

        @Override
        public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
            return new CloseableIterator<SerializedEventWithToken>() {
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < events.length;
                }

                @Override
                public SerializedEventWithToken next() {
                    SerializedEventWithToken serializedEventWithToken = new SerializedEventWithToken(index,
                                                                                                     events[index]);
                    index++;
                    return serializedEventWithToken;
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint... hints) {
            return Optional.of(7L);
        }

        @Override
        public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
            for (int i = 0; i < events.length; i++) {
                if (!consumer.test(EventWithToken.newBuilder()
                                                 .setToken(i).setEvent(events[i]).build())) {
                    return;
                }
            }
        }

        @Override
        public CompletableFuture<Long> store(List<Event> eventList) {
            if (eventList.get(0).getMessageIdentifier().equals("FAIL")) {
                CompletableFuture<Long> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(new RuntimeException("Empty transaction"));
                return completableFuture;
            }
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public void processEventsPerAggregateHighestFirst(String aggregateId, long actualMinSequenceNumber,
                                                          long actualMaxSequenceNumber, int maxResults,
                                                          Consumer<SerializedEvent> eventConsumer) {
            for (int i = events.length - 1; i >= 0; i--) {
                eventConsumer.accept(new SerializedEvent(events[i]));
            }
        }

        @Override
        public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber) {
            return Optional.of(new SerializedEvent(events[events.length - 1]));
        }
    }
}